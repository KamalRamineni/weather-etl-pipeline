import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)


def handler(event, context):
    """
    AWS Lambda entry point.

    Triggered by:
    - EventBridge schedule (hourly): event = {}
    - Manual test:                   event = {"export_csv": false}

    Returns a standard API Gateway-shaped dict so the response is readable
    in both the Lambda console and CloudWatch.
    """
    logger.info("Invoked — event: %s", json.dumps(event))

    from main import WeatherETLPipeline

    try:
        pipeline = WeatherETLPipeline()
        stats = pipeline.run_pipeline(export_csv=event.get('export_csv', False))

        if stats['success']:
            body = {
                'status': 'success',
                'extracted': stats['extracted'],
                'transformed': stats['transformed'],
                'loaded': stats['loaded'],
                's3_key': stats.get('s3_key'),
                'duration_seconds': round(stats['duration_seconds'], 2),
            }
            logger.info("Pipeline succeeded: %s", body)
            return {'statusCode': 200, 'body': json.dumps(body)}

        body = {'status': 'failed', 'errors': stats['errors']}
        logger.error("Pipeline failed: %s", body)
        return {'statusCode': 500, 'body': json.dumps(body)}

    except Exception as e:
        logger.exception("Unhandled error in Lambda handler")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
