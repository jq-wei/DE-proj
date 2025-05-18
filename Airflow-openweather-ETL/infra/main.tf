provider "aws" {
  region = "us-east-1"
}


# data source to ge the default VPC
data "aws_vpc" "default" {
  default = true
}


#create security group to allow port 22,80, 443

resource "aws_security_group" "web_sg" {
  name        = "allow_http_https"
  description = "Allow HTTP and HTTPS"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Allow TCP 8080 from anywhere"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Allows any IP
  }


  ingress {
    description = "HTTPS from VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # all ip can access
  }

  ingress {
    description = "HTTP from VPC"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # all ip can access
  }

  ingress {
    description = "HTTPS from V"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # all ip can access
  }

  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1" # any protocol
    cidr_blocks = ["0.0.0.0/0"] # all ip 
  }

  tags = {
    Name = "allow_web"
  }
}


# create ubuntu server and install/enable apache2 */

resource "aws_instance" "web-server-instance" {
  ami = "ami-084568db4383264d4"
  instance_type = "t3.medium" #"t2.micro"
  availability_zone = "us-east-1a" # same as subnet
  key_name = "main-key"

  vpc_security_group_ids = [ aws_security_group.web_sg.id ]

  tags = {
    Name = "airflow-server"
  }
}



# sudo apt update
# sudo apt install python3-pip
            # sudo apt install software-properties-common
            # sudo add-apt-repository ppa:deadsnakes/ppa
# sudo apt install python3.12-venv
# python3 -m venv airflow_venv
# source airflow_venv/bin/activate
# pip install pandas
# pip install apache-airflow
