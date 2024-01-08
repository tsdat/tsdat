# Deploying to AWS

!!! danger

    This deployment can only be run by AWS administrators, so we assume the user has a basic understanding of code
    development, Docker containers, and the AWS cloud.

The **[tsdat/aws-template](https://github.com/tsdat/aws-template)** repository contains everything needed to deploy your
tsdat pipelines to Amazon Web Services (AWS).

## Overview

### Architecture

The following pictures give a high level overview of the build process and
what resources are created in AWS.

<!-- ![Image](./images/aws_template.png)
![Image](./images/tsdat-aws-code-build.png) 
![Image](./images/tsdat-aws-functional-diagram.png) -->

**TODO**

## Prerequisites

### Get an AWS Account

In order to deploy resources to AWS, you must have an account set up and you must have administrator privileges on that
account.  If you do not have an AWS account or you do not have admin privileges, then you should contact the local cloud
administrator for your organization.

### Create code repos

Make sure that you have two repositories in your GitHub organization created from the following templates:

1. <https://github.com/tsdat/pipeline-template>
2. <https://github.com/tsdat/aws-template>

If you are using an existing pipelines repository, make sure that the `requirements.txt` file has the tsdat version at
`tsdat==0.7.1` or greater. The AWS build will not work with earlier versions of tsdat.

Clone these repos to the same parent folder on your computer.

??? warning "Warning: Windows users"

    If you are using WSL on Windows make sure you run the `git clone` command from a WSL terminal.

### Install Docker

We use a Docker container with VSCode to make setting up your development environment a snap.  We assume users have a
basic familiarity with Docker containers. If you are new to Docker, there are many free online tutorials to get you
started.

!!! tip

    Docker Desktop can be flaky, especially on Windows, and it requires a license so we recommend not using it. Instead,
    we are providing alternative, non-Docker Desktop installation instructions for each platform. The Docker Desktop
    install is easier and requires fewer steps, so it may be fine for your needs, but keep in mind it may crash if you
    update it (requiring a full uninstall/reinstall, and then you lose all your container environments).

    === "Windows Users"

        * [Install Docker on wsl2](https://github.com/clansing/docs/blob/main/windows-docker-wsl2.md)
        * [Install Docker Desktop](https://docs.docker.com/desktop/install/windows-install/)
    === "Mac Users"

        * [Use Docker/Colima](https://dev.to/elliotalexander/how-to-use-docker-without-docker-desktop-on-macos-217m)
        * [Install Docker Desktop](https://docs.docker.com/desktop/install/mac-install/)
    === "Linux Users"

        * [Install Docker](https://docs.docker.com/engine/install/ubuntu/)
        * [Install Docker Desktop](https://docs.docker.com/desktop/install/linux-install/)

We also recommend [installing VS Code](https://code.visualstudio.com/download) and using the
[ms-vscode-remote.vscode-remote-extensionpack](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack)
extension, which includes support for editing code in Docker Containers.

## Deploying your AWS Stack

### Open your `aws-template` repo in VS Code

!!! note "Note: Windows"
    If you are using WSL on Windows then you MUST open VSCode from within a WSL terminal in order for VSCode to
    automatically install the proper WSL interface extension.

For example, if you checked out the aws-template repository to your `$HOME/projects/tsdat` folder, then you would run
this to start VSCode:

```shell
cd $HOME/projects/tsdat
code aws-template
```

### Start your tsdat-cdk Docker container

From your VSCode window, start a terminal (^^Main Menu -> Terminal -> New^^, OR you can press ++ctrl+grave++).

Then from the VSCode terminal, run:

 ```shell
 docker pull nikolaik/python-nodejs:python3.11-nodejs20
 docker compose up -d
 ```

### Attach a new VSCode window to the tsdat-cdk container

1. Type the key combination:  ++ctrl+shift+p++ to bring up the VSCode command palette.
1. Then from the input box type: "Dev-Containers:  Attach to Running Container..." and select it
1. Then choose the  tsdat-cdk  container.

This will start up a new VSCode window that is running from inside your tsdat-cdk container.

### Open the provided cdk.code-workspace file

From the VSCode window that is attached to the tsdat-cdk container:

    Main Menu -> File-> Open Workspace from File...
    In the file chooser dialog, select ```/root/aws-template/.vscode/cdk.code-workspace```

!!! tip

    A box should pop up in the bottom right corner that asks if you want to install the recommended extensions.
    Select "Install".

Once the extensions are installed, your workspace is ready! In the Explorer, you will see two folders:

* aws-template
* .aws

### Edit your deployed pipelines config file

Do this from the VSCode window that is attached to the tsdat-cdk container. Open the
`aws-template/pipelines_config.yml` file and make sure to fill out all the sections
(build parameters and pipelines).

To create an AWS CodeStar Connection to GitHub follow the steps
[**here**](https://docs.aws.amazon.com/dtconsole/latest/userguide/connections-create-github.html#connections-create-github-console)
You'll need to add the ARN of your CodeStar connection to this file.

TODO: Add more info to this section

* GitHub repo/org info
* AWS Account & Bucket Info
* AWS -> GitHub CodeStar Connection
* Configurations for each pipeline to deploy (with examples)

### Configure your tsdat AWS profile

From a terminal inside your VSCode window attached to the docker container run this command:

```shell
aws configure --profile tsdat
# AWS Access Key ID [X3EN]: 
# AWS Secret Access Key [6o89]: 
# Default region name [None]: us-west-2
# Default output format [None]: 
```

Your `~/.aws/config` file should now look like this:

```txt
[profile tsdat]
region = us-west-2
```

### Edit your aws credentials

CDK requires that your AWS credentials be set in order to authenticate your CLI actions.

<!-- ```diff
!  NOTE:  You must use AWS credentials file, NOT the PNNL SSO login, which is not supported by the CDK.
``` -->

!!! tip

    You will need to do this step BEFORE you deploy your stack and any time the credentials expire (usually after about
    12 hours).

From your VSCode window that is attached to the tsdat-cdk container:

* From the Explorer view, open the .aws/credentials file.  
* Then go to the AWS login page (<https://pnnl.awsapps.com/start>)
* Then click $PROJECT_NAME -> Administrator -> Command line or programmatic access  (use whatever project you are admin for)
* In the section, "Option 2: Manually add a profile to your AWS credentials file (Short-term credentials)", Click on the box to copy the text.
* Paste it in  your credentials file under the `[tsdat]` profile (make sure to delete the line `[xxxxx _AdministratorAccess]`)

Your credentials file should look like this (with real values instead of the XXXX):

```txt
[tsdat]
aws_access_key_id=XXXXXXX
aws_secret_access_key=XXXXXX
aws_session_token=XXXXXX
```

### Run the cdk bootstrap (Only ONCE for your AWS Account/Region!)

Bootstrapping is the process of provisioning resources for the AWS CDK before you can
deploy AWS CDK apps into an AWS environment. (An AWS environment is a combination of an
AWS account and Region).

These resources include an Amazon S3 bucket for storing files and IAM roles that grant
permissions needed to perform deployments.

The required resources are defined in an AWS CloudFormation stack, called the bootstrap
stack, which is usually named CDKToolkit. Like any AWS CloudFormation stack, it appears
in the AWS CloudFormation console once it has been deployed.

Check your Cloud Formation stacks first to see if you need to deploy the bootstrap.
(e.g., <https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2>)
If you see a stack named `CDKToolkit`, then you can SKIP this step.

```shell
cd aws-template
./bootstrap_cdk.sh
```

### Run the cdk build

You can re-run this for each branch you want to deploy (e.g., dev, prod, etc.) and any time
you make changes to the stack (for example, you add a new permission to your lambda role).

!!! note

    Most deployments will not need to change anything in the stack, but advanced users are free to customize.

```shell
cd aws-template
./deploy_stack.sh $BRANCH   (where $BRANCH is the branch you want to deploy (e.g., dev/prod))
```

## Viewing your Resources in AWS

You can use the AWS UI to view the resources that were created via the build.

### Code Pipeline

From here you can check the status of your code build to make sure it is running successfully.

<https://us-west-2.console.aws.amazon.com/codesuite/codepipeline/pipelines/>

### ECR Container Repository

From here you can check the status of your built images.

<https://us-west-2.console.aws.amazon.com/ecr/repositories?region=us-west-2>

### S3 Buckets

<https://s3.console.aws.amazon.com/s3/buckets?region=us-west-2>

### Lambda Functions

You can see the lambda functions that were created for each pipeline here.

<https://us-west-2.console.aws.amazon.com/lambda/home?region=us-west-2#/functions>

### Event Bridge Cron Rules

<https://us-west-2.console.aws.amazon.com/events/home?region=us-west-2#/rules>

### Cloud Formation Stack

You can see the resources that were created via the CDK deploy.  You can also delete
the stack from here to clean up those resources.  Note that any lambda functions and
Event Bridge cron rules created via the CodePipeline build are NOT part of the stack,
so these would have to be removed by hand.

<https://us-west-2.console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks?filteringText=&filteringStatus=active&viewNested=true>
