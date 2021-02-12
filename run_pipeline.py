from datetime import datetime

from jobs.tracks_statistics.report_generator import DataReportGenerator
from dependencies.spark import start_spark


def main():
    spark, log, config = start_spark(
        app_name='top_most_played_tracks')

    # log that main ETL job is starting
    log.warn('job is up-and-running')

    run_id = datetime.now().strftime('%Y%m%d%H%M%S')
    output_file_path = f'/app/data/output/' \
                       f'top_most_played_tracks_{run_id}.csv'
    input_file_path = '/app/data/input/' \
                      'userid-timestamp-artid-artname-traid-traname.tsv'

    data_insights = DataReportGenerator(spark)
    data_insights.top_tracks_for_top_sessions(
            input_file_path=input_file_path,
            output_file_path=output_file_path,
            top_tracks_number=10,
            top_sessions_number=50
    )

    log.warn('job is finished')
    spark.stop()


if __name__ == '__main__':
    main()
