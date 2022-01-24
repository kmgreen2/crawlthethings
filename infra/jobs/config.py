import pulumi

config = pulumi.Config('jobs')
vpc_id = config.require('vpc_id')
subnet_cidr = config.require('subnet_cidr')
nat_subnet_cidr = config.require('nat_subnet_cidr')
instance_type = config.require('instance_type')
ssm_arn = config.require('ssm_arn')
public_key = config.get('public_key')
