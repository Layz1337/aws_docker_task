#!/usr/bin/env python3

import logging
import argparse
from datetime import datetime
from typing import Any

import docker
import boto3
from docker.errors import DockerException, APIError
from botocore.exceptions import ClientError, ConnectionError


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MAX_CONNECTION_RETRIES = 5


def parse_arguments() -> argparse.Namespace:
    """
        Parse command line arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--docker-image',
        type=str,
        help='Docker image name'
    )
    parser.add_argument(
        '--bash-command',
        type=str,
        help='Bash command to run in the Docker container'
    )
    parser.add_argument(
        '--aws-cloudwatch-group',
        type=str,
        help='AWS CloudWatch group'
    )
    parser.add_argument(
        '--aws-cloudwatch-stream',
        type=str,
        help='AWS CloudWatch stream'
    )
    parser.add_argument(
        '--aws-access-key-id',
        type=str,
        help='AWS access key ID'
    )
    parser.add_argument(
        '--aws-secret-access-key',
        type=str,
        help='AWS secret access key'
    )
    parser.add_argument(
        '--aws-region',
        type=str,
        help='AWS region'
    )

    args = parser.parse_args()
    return args


def init_docker_client() -> docker.DockerClient:
    """
        Initialize Docker client
    """
    logger.info('Initializing Docker client')
    try:
        return docker.from_env()
    except DockerException as e:
        # Handle Docker connection error
        logger.error(
            f'An error occurred while initializing Docker client: {e}'
        )
        raise


def run_docker_container(
        docker_client: docker.DockerClient,
        image_name: str,
        command_to_run: str
) -> docker.models.containers.Container:
    """
        Run Docker container with the specified image and bash command
    """
    logger.info(
        f'Running Docker container with image {image_name} in detached mode'
    )
    try:
        return docker_client.containers.run(
            image=image_name,
            command=command_to_run,
            detach=True
        )
    except APIError as e:
        # Handle errors from Docker API
        logger.error(
            f'An error occurred while running Docker container: {e}'
        )
        raise


def init_cw_client(
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region_name: str
) -> Any:
    """
        Initialize AWS CloudWatch client
    """
    logger.info('Initializing AWS CloudWatch client')
    return boto3.client(
        service_name='logs',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )


def ensure_cw_log_group_and_stream(
        cw_client: Any,
        log_group_name: str,
        log_stream_name: str
) -> None:
    """
        Ensure that the specified CloudWatch log group and stream exist
        It reraises the exception if it's not a ResourceAlreadyExistsException
    """
    logger.info(
        f'Ensuring that the CloudWatch log group {log_group_name} and stream '
        f'{log_stream_name} exist'
    )

    try:
        cw_client.create_log_group(
            logGroupName=log_group_name
        )

    except ClientError as e:
        # If the log group or stream already exists, just continue
        # otherwise, re-raise the exception
        if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
            logger.error(
                'An error occurred while ensuring the CloudWatch log '
                f'group exist: {e}'
            )
            raise

    try:
        cw_client.create_log_stream(
            logGroupName=log_group_name,
            logStreamName=log_stream_name
        )

    except ClientError as e:
        # If the log group or stream already exists, just continue
        # otherwise, re-raise the exception
        if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
            logger.error(
                'An error occurred while ensuring the CloudWatch log '
                f'stream exist: {e}'
            )
            raise


def push_log_event_to_cw_repeatedly(
        cw_client: Any,
        log_group_name: str,
        log_stream_name: str,
        log_events: dict
) -> None:
    """
        Push the log event to CloudWatch
        It retries in case if network error occurs
    """

    # Retry if network error occurs
    for _ in range(MAX_CONNECTION_RETRIES):
        try:
            # Push the log event to CloudWatch
            cw_client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=log_events
            )
            return

        except ConnectionError:
            if _ == MAX_CONNECTION_RETRIES - 1:
                # Max retries reached
                logger.error(
                    'Max retries reached while pushing log events to '
                    'CloudWatch'
                )
                raise
            # Retry if network error occurs
            logger.error(
                'A network error occurred while pushing log events to '
                'CloudWatch'
            )
            continue


def stream_and_push_logs(
        container: docker.models.containers.Container,
        cw_client: Any,
        log_group_name: str,
        log_stream_name: str
) -> None:
    """
        Stream container logs and push them to CloudWatch in real-time
    """
    for line in container.logs(stream=True, follow=True, stdout=True, stderr=True):
        message = line.decode('utf-8')
        timestamp = int(datetime.now().timestamp() * 1000)
        log_event = {
            'timestamp': timestamp,
            'message': message
        }

        push_log_event_to_cw_repeatedly(
            cw_client=cw_client,
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
            log_events=[log_event]
        )


def main():
    container = None

    try:
        # Parse command line arguments
        args = parse_arguments()

        # Initialize Docker client
        docker_client = init_docker_client()

        # Initialize AWS CloudWatch client
        cw_client = init_cw_client(
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region_name=args.aws_region
        )

        # Ensure the specified CloudWatch log group and stream
        ensure_cw_log_group_and_stream(
            cw_client,
            args.aws_cloudwatch_group,
            args.aws_cloudwatch_stream
        )

        # Run the Docker container
        container = run_docker_container(
            docker_client,
            args.docker_image,
            ['bash', '-c', args.bash_command]  # wrap command using bash -c
        )

        # Stream and push logs to CloudWatch
        stream_and_push_logs(
            container,
            cw_client,
            args.aws_cloudwatch_group,
            args.aws_cloudwatch_stream
        )

    except Exception as e:
        logger.error(f'An error occurred: {e}')
        raise

    except KeyboardInterrupt:
        logger.info(
            'The process was interrupted by the user'
        )

    finally:
        # If the container is not exist, just exit
        if container is None:
            return

        logging.info('Stopping and removing the Docker container')

        try:
            if container.status == 'running':
                # Stop the container if it's still running
                container.stop(
                    # Wait for 10 seconds for the container to stop
                    timeout=10
                )

            # Force removal to ensure the container is removed
            container.remove(force=True)

        except DockerException as e:
            logger.error(
                'An error occurred while stopping/removing the '
                f'Docker container: {e}'
            )


if __name__ == "__main__":
    main()
