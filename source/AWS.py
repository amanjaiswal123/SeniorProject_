import boto3
import paramiko
from config.config import qtheus_rds
import psycopg2

def Start_Instance(instance_id:str,wait_until_start=True):
    #Must pass the ID of the instance you want to connect too as instance_id. if you don't want to wait until the instance has started set wait_until_start to False
    ec2 = boto3.resource('ec2')  # Create instance of boto3 resource
    client = boto3.client('ec2')  # Create client instance for boto3
    client.start_instances(InstanceIds=[instance_id])  # Start the EC2 using the instance
    instance = ec2.Instance(instance_id)  # Get info of EC2 Instance using the existing boto 3 resource Instance
    if wait_until_start is True:
        instance.wait_until_running()  # Wait until the instance is running
    return client #Return an instance of the client's connection to the AWS instance
def ssh(instance_ID:str,path_to_key:str,user:str):
    #Must pass the ID of the instance you want to connect too as instance_ID. Must Pass a path to the .pem key for that instance as path_to_key. By default the user is
    # ubuntu can change by changing user argument
    ec2 = boto3.resource('ec2')  # Create instance of boto3 resource
    instance = ec2.Instance(instance_ID)  # Get info of EC2 Instance using the existing boto 3 resource Instance
    instanceIP = instance.public_dns_name  # Get the public DNS of the instance that we will use to ssh
    key = paramiko.RSAKey.from_private_key_file(path_to_key)  # Load the private key to ssh into the EC2 instance
    sshclient = paramiko.SSHClient()  # Create a parmiko SSH Client instance
    sshclient.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # IDK what this does the guy on stack over flow did it
    sshclient.connect(hostname=instanceIP, username=user,pkey=key)  # Connect the EC2 instance using paramiko SSH Client instance
    return sshclient #returning instance of the client's ssh connection
def scpupload(client,Sending_Path:str,Recieving_Path:str):
    #Must pass an instance of the client's ssh connection as client arg, Must pass the path to the file you want to send as Sending_Path, and the path to the save the file to as Recieving_Path
    sftp = client.open_sftp()  # IDK what this does saw it on stack over flow I think this creates a instance of the sftp connection
    sftp.put(Sending_Path,Recieving_Path)  # SCPING Data
    sftp.close()  # I think this ends the instance of the connection
def scpdownload(client,Sending_Path:str,Recieving_Path:str):
    sftp = client.open_sftp()  # IDK what this does saw it on stack over flow I think this creates a instance of the sftp connection
    sftp.get(Sending_Path,Recieving_Path)  # SCPING Data
    sftp.close()  # I think this ends the instance of the connection

def get_max_date(table):
    try:
        conn = psycopg2.connect(dbname='postgres', user=qtheus_rds['user'], host=qtheus_rds['host'],password=qtheus_rds['password'])
    except:
        raise Exception("Unable to connect to the database")
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(date) FROM {table}")
    date = cursor.fetchone()[0]
    conn.close()
    cursor.close()
    return str(date.date())
