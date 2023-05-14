resource "aws_s3_bucket" "example" {
  bucket = "udacity-datalake-eshwar"
  acl    = "public"

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}