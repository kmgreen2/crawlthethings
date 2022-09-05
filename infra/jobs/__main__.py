import pulumi
import pulumi_aws as aws
import config

ecs_instance_role_role = aws.iam.Role("ecsInstanceRoleRole", assume_role_policy="""{
    "Version": "2012-10-17",
    "Statement": [
	{
	    "Action": "sts:AssumeRole",
	    "Effect": "Allow",
	    "Principal": {
	        "Service": "ec2.amazonaws.com"
	    }
	}
    ]
}
""")

ecs_instance_role_role_policy_attachment = aws.iam.RolePolicyAttachment("ecsInstanceRoleRolePolicyAttachment",
    role=ecs_instance_role_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role")

ssm_role_policy_attachment = aws.iam.RolePolicyAttachment("ssmRolePolicyAttachment",
    role=ecs_instance_role_role.name,
    policy_arn=config.ssm_arn)

ecs_instance_role_instance_profile = aws.iam.InstanceProfile("ecsInstanceRoleInstanceProfile", role=ecs_instance_role_role.name)
aws_batch_service_role_role = aws.iam.Role("awsBatchServiceRoleRole", assume_role_policy="""{
    "Version": "2012-10-17",
    "Statement": [
	{
	    "Action": "sts:AssumeRole",
	    "Effect": "Allow",
	    "Principal": {
		"Service": "batch.amazonaws.com"
	    }
	}
    ]
}
""")
aws_batch_service_role_role_policy_attachment = aws.iam.RolePolicyAttachment("awsBatchServiceRoleRolePolicyAttachment",
    role=aws_batch_service_role_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole")

security_group = aws.ec2.SecurityGroup("crawlTheThingsVpcSecurityGroup",
    vpc_id=config.vpc_id,
    egress=[aws.ec2.SecurityGroupEgressArgs(
        from_port=0,
        to_port=0,
        protocol="-1",
        cidr_blocks=["0.0.0.0/0"],
    )],
    ingress=[aws.ec2.SecurityGroupIngressArgs(
        to_port=22,
        from_port=22,
        protocol="tcp",
        cidr_blocks=["0.0.0.0/0"])
    ])

subnet = aws.ec2.Subnet("crawlTheThingsSubnet",
                        vpc_id=config.vpc_id,
                        map_public_ip_on_launch=True,
                        cidr_block=config.subnet_cidr)

igw = aws.ec2.InternetGateway("igw",
    vpc_id=config.vpc_id,
    tags={
        "Name": "igw",
    })


egress_routes = aws.ec2.RouteTable("egressRoutes",
    vpc_id=config.vpc_id,
    routes=[
        aws.ec2.RouteTableRouteArgs(
            cidr_block='0.0.0.0/0',
            gateway_id=igw.id
        ),
    ],
    tags={
        "Name": "egressRoutes",
    })

egress_route_association = aws.ec2.RouteTableAssociation("egressRouteAssociation",
    subnet_id=subnet.id,
    route_table_id=egress_routes.id)

import pulumi
import pulumi_aws as aws

key_pair_id = None
if config.public_key is not None:
    key_pair = aws.ec2.KeyPair("key_pair", public_key=config.public_key)
    key_pair_id = key_pair.id

compute_environment = aws.batch.ComputeEnvironment("crawlTheThings",
    compute_environment_name="crawlTheThingsEnv",
    compute_resources=aws.batch.ComputeEnvironmentComputeResourcesArgs(
        instance_role=ecs_instance_role_instance_profile.arn,
        instance_types=[config.instance_type],
        image_id='ami-0f9328f51fa0b77cf',
        ec2_key_pair=key_pair_id,
        max_vcpus=32,
        desired_vcpus=16,
        min_vcpus=0,
        security_group_ids=[security_group.id],
        subnets=[subnet.id],
        type="EC2",
    ),
    service_role=aws_batch_service_role_role.arn,
    type="MANAGED",
    opts=pulumi.ResourceOptions(depends_on=[aws_batch_service_role_role_policy_attachment]))

queue = aws.batch.JobQueue("crawlTheThings",
    state="ENABLED",
    priority=1,
    compute_environments=[
        compute_environment.arn
    ])