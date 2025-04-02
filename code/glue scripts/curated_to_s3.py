import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging
from datetime import datetime

# Initialize logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize Glue context
try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Get job parameters
    args = getResolvedOptions(
        sys.argv, 
        [
            'JOB_NAME',
            'OUTPUT_BUCKET',
            'DATABASE_NAME'
        ]
    )
    
    job.init(args['JOB_NAME'], args)
    
    # Configuration from parameters
    output_bucket = args['OUTPUT_BUCKET']
    database_name = args['DATABASE_NAME']
    
    logger.info(f"Job initialized with output bucket: {output_bucket} and database: {database_name}")
    
    # Utility function to write data to S3 with error handling
    def write_data(df, path, format="parquet", mode="overwrite"):
        try:
            df.write.mode(mode).format(format).save(path)
            logger.info(f"Successfully wrote data to {path}")
            return True
        except Exception as e:
            logger.error(f"Error writing data to {path}: {str(e)}")
            raise
    
    # 1. Read raw data from Glue Data Catalog with error handling
    def read_table(table_name):
        try:
            logger.info(f"Reading table {table_name} from Glue catalog")
            df = glueContext.create_dynamic_frame.from_catalog(
                database=database_name,
                table_name=table_name
            ).toDF()
            logger.info(f"Successfully read table {table_name} with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}")
            raise
    
    try:
        apartments = read_table("apartments")
        apartment_attributes = read_table("apartments_attributes")
        bookings = read_table("bookings")
        user_viewing = read_table("user_viewing")
    except Exception as e:
        logger.error("Failed to read one or more source tables")
        raise
    
    # 2. Apply transformations with error handling
    try:
        logger.info("Starting data transformations")
        
        # Update currency to 'USD'
        apartments = apartments.withColumn("currency", lit("USD"))
        bookings = bookings.withColumn("currency", lit("USD"))
        
        # Standardize Dates
        my_date_format = 'dd/MM/yyyy'
        apartments = apartments.withColumn(
            "listing_created_on", 
            to_date(col("listing_created_on"), my_date_format)
        ).withColumn(
            "last_modified_timestamp", 
            to_date(col("last_modified_timestamp"), my_date_format)
        )
        
        bookings = bookings.withColumn(
            "booking_date", 
            to_date(col("booking_date"), my_date_format)
        ).withColumn(
            "checkin_date", 
            to_date(col("checkin_date"), my_date_format)
        ).withColumn(
            "checkout_date", 
            to_date(col("checkout_date"), my_date_format)
        )
        
        user_viewing = user_viewing.withColumn(
            "viewed_at", 
            to_date(col("viewed_at"), my_date_format)
        )
        
        # Drop duplicates
        apartments = apartments.dropDuplicates()
        apartment_attributes = apartment_attributes.dropDuplicates()
        user_viewing = user_viewing.dropDuplicates()
        bookings = bookings.dropDuplicates()
        
        # Combine apartments and apartments attributes data
        curated_apartments = apartments.join(
            apartment_attributes, 
            apartments.id == apartment_attributes.id
        ).drop(apartment_attributes.id)
        
        # Calculate stay duration and booking week
        curated_bookings = bookings.withColumn(
            "stay_days",
            abs(datediff(col("checkout_date"), col("checkin_date")))
        ).withColumn(
            "booking_week",
            weekofyear(col("booking_date"))
        )
        
        # Create curated user viewing (no transformations needed)
        curated_user_viewing = user_viewing
        
        logger.info("Data transformations completed successfully")
    except Exception as e:
        logger.error(f"Error during data transformations: {str(e)}")
        raise
    
    # 4. Calculate and write business metrics
    try:
        logger.info("Calculating business metrics")
        
        # Utility function for writing metrics
        def write_metrics(df, metric_name):
            path = f"s3://{output_bucket}/presentation_layer_data/{metric_name}/"
            write_data(df, path)
            return path
        
        # Average Listing Price
        weekly_avg_price = curated_apartments.filter(col("is_active") == True) \
            .groupBy(
                year("listing_created_on").alias("year"),
                weekofyear("listing_created_on").alias("week")
            ) \
            .agg(
                avg("price").alias("average_price"),
                date_format(min("listing_created_on"), "dd/MM/yyyy").alias("week_start_date"),
                date_format(max("listing_created_on"), "dd/MM/yyyy").alias("week_end_date")
            ).orderBy("year", "week")
        
        avg_price_path = write_metrics(weekly_avg_price, "average_listing_price")
        logger.info(f"Average listing price metrics written to {avg_price_path}")
        
        # Occupancy Rate
        monthly_occupancy = (
            # Generate all months in range
            spark.range(1).select(
                explode(sequence(to_date(lit('2020-01-01')), to_date(lit('2025-12-31')), expr("interval 1 month"))).alias("month")
            )
            # Calculate active listings per month
            .join(
                curated_apartments.filter(col("is_active")),
                how="cross"
            )
            .groupBy(
                year(col("month")).alias("year"),
                month(col("month")).alias("month"),
                day(last_day(col("month"))).alias("days_in_month")
            )
            .agg(count("*").alias("active_listings"))
            # Join with bookings data
            .join(
                curated_bookings.filter(col("booking_status") == "confirmed")
                    .groupBy(
                        year(col("checkin_date")).alias("year"),
                        month(col("checkin_date")).alias("month")
                    )
                    .agg(
                        sum(datediff(col("checkout_date"), col("checkin_date"))).alias("total_booked_nights"),
                        count("*").alias("total_bookings")
                    ),
                ["year", "month"],
                "left"
            )
            # Calculate metrics
            .fillna(0)
            .withColumn("available_nights", col("active_listings") * col("days_in_month"))
            .withColumn("occupancy_rate", round(col("total_booked_nights") / col("available_nights") * 100, 2))
            .select(
                "year", "month", "active_listings", "days_in_month",
                "available_nights", "total_bookings", "total_booked_nights", "occupancy_rate"
            )
            .orderBy("year", "month")
        )
        
        occupancy_path = write_metrics(monthly_occupancy, "occupancy_rate")
        logger.info(f"Occupancy rate metrics written to {occupancy_path}")
        
        # Most Popular Locations
        window_spec = Window.partitionBy("year", "week").orderBy(col("booking_count").desc())
        popular_locations = curated_bookings.filter(col('booking_status') == 'confirmed') \
            .join(curated_apartments, curated_bookings.apartment_id == curated_apartments.id) \
            .groupBy(
                year(col('booking_date')).alias('year'),
                weekofyear(col('booking_date')).alias('week'),
                col('cityname')
            ).agg(count('*').alias('booking_count')) \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") <= 5) \
            .orderBy("year", "week", "rank")
        
        popular_locs_path = write_metrics(popular_locations, "popular_locations")
        logger.info(f"Popular locations metrics written to {popular_locs_path}")
        
        # Top Performing Listings
        top_perf_window = Window.partitionBy("year", "week").orderBy(desc("week_revenue"))
        top_performing_listings = curated_bookings.filter(col('booking_status') == 'confirmed') \
            .join(curated_apartments, curated_bookings.apartment_id == curated_apartments.id) \
            .groupBy(
                year(col('booking_date')).alias('year'),
                weekofyear(col('booking_date')).alias('week'),
                col('id'),
                col('title'),
                col('cityname')
            ).agg(sum('total_price').alias('week_revenue')) \
            .withColumn("rank", rank().over(top_perf_window)) \
            .filter(col("rank") <= 5) \
            .orderBy("year", "week", "rank")
        
        top_perf_path = write_metrics(top_performing_listings, "top_performing_listings")
        logger.info(f"Top performing listings metrics written to {top_perf_path}")
        
        # User Engagement Metrics
        # Total Bookings per User
        user_weekly_bookings = curated_bookings.filter(col('booking_status') == 'confirmed') \
            .groupBy(
                year('booking_date').alias('year'),
                weekofyear('booking_date').alias('week'),
                'user_id'
            ).agg(count('booking_id').alias('total_bookings')) \
            .orderBy('year', 'week', col('total_bookings').desc())
        
        user_bookings_path = write_metrics(user_weekly_bookings, "user_weekly_bookings")
        logger.info(f"User bookings metrics written to {user_bookings_path}")
        
        # Average Booking Duration
        avg_booking_duration = curated_bookings.filter(col('booking_status') == 'confirmed') \
            .withColumn('duration_nights', datediff('checkout_date', 'checkin_date')) \
            .groupBy(
                year('checkin_date').alias('year'),
                weekofyear('checkin_date').alias('week')
            ).agg(avg('duration_nights').alias('avg_booking_duration')) \
            .orderBy('year', 'week')
        
        avg_duration_path = write_metrics(avg_booking_duration, "average_booking_duration")
        logger.info(f"Average booking duration metrics written to {avg_duration_path}")
        
        # Repeat Customer Rate
        monthly_repeats = curated_bookings.filter(col('booking_status') == 'confirmed') \
            .groupBy(
                year('booking_date').alias('year'),
                month('booking_date').alias('month'),
                'user_id'
            ).agg(count('*').alias('bookings')) \
            .groupBy('year', 'month') \
            .agg(
                count('user_id').alias('total_users'),
                sum(when(col('bookings') > 1, 1).otherwise(0)).alias('repeat_users'),
                round(sum(when(col('bookings') > 1, 1).otherwise(0)) / count('user_id') * 100, 2).alias('repeat_rate')
            ).orderBy('year', 'month')
        
        repeat_rate_path = write_metrics(monthly_repeats, "repeat_customer_rate")
        logger.info(f"Repeat customer rate metrics written to {repeat_rate_path}")
        
    except Exception as e:
        logger.error(f"Error calculating or writing business metrics: {str(e)}")
        raise
    
    # 3. Write curated data to S3
    try:
        logger.info("Writing curated data to S3")
        write_data(curated_apartments, f"s3://{output_bucket}/curated_layer_data/curated_apartments/")
        write_data(curated_bookings, f"s3://{output_bucket}/curated_layer_data/curated_bookings/")
        write_data(curated_user_viewing, f"s3://{output_bucket}/curated_layer_data/curated_user_viewing/")
    except Exception as e:
        logger.error("Failed to write curated data to S3")
        raise
    
    job.commit()
    logger.info("Glue job completed successfully")
    
except Exception as e:
    logger.error(f"Glue job failed: {str(e)}")
    raise