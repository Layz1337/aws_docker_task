# Task  
Run the docker container with the given bash command, capture container logs, and send them to the AWS CloudWatch group/stream

# Usage
``` bash
usage: main.py [-h] --docker-image DOCKER_IMAGE --bash-command BASH_COMMAND --aws-cloudwatch-group AWS_CLOUDWATCH_GROUP --aws-cloudwatch-stream AWS_CLOUDWATCH_STREAM
               --aws-access-key-id AWS_ACCESS_KEY_ID --aws-secret-access-key AWS_SECRET_ACCESS_KEY --aws-region AWS_REGION [--log-level {debug,info,warning,error,critical}]

options:
  -h, --help            show this help message and exit
  --docker-image DOCKER_IMAGE
                        Docker image name
  --bash-command BASH_COMMAND
                        Bash command to run in the Docker container
  --aws-cloudwatch-group AWS_CLOUDWATCH_GROUP
                        AWS CloudWatch group
  --aws-cloudwatch-stream AWS_CLOUDWATCH_STREAM
                        AWS CloudWatch stream
  --aws-access-key-id AWS_ACCESS_KEY_ID
                        AWS access key ID
  --aws-secret-access-key AWS_SECRET_ACCESS_KEY
                        AWS secret access key
  --aws-region AWS_REGION
                        AWS region
  --log-level {debug,info,warning,error,critical}
                        Set the logging level
```
