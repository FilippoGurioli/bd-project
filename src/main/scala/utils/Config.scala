package utils

object Config {

  // The local directory containing this repository
  val projectDir :String = "/home/filippo/Documents/Unibo/BD/bd-project"
  // The name of the shared bucket on AWS S3 to read datasets
  val s3sharedBucketName :String = "unibo-31-10-fgurioli"
  // The name of your bucket on AWS S3
  val s3bucketName :String = "unibo-31-10-fgurioli"
  // The path to the credentials file for AWS (if you follow instructions, this should not be updated)
  val credentialsPath :String = "/aws_credentials.txt"

}
