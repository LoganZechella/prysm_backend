resource "aws_iam_role" "personalize_role" {
  name = "prysm-personalize-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "personalize.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "personalize_policy" {
  name        = "prysm-personalize-policy"
  description = "IAM policy for Amazon Personalize access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "personalize:CreateDataset",
          "personalize:CreateDatasetGroup",
          "personalize:CreateDatasetImportJob",
          "personalize:CreateEventTracker",
          "personalize:CreateRecommender",
          "personalize:CreateSchema",
          "personalize:DeleteDataset",
          "personalize:DeleteDatasetGroup",
          "personalize:DeleteEventTracker",
          "personalize:DeleteRecommender",
          "personalize:DeleteSchema",
          "personalize:DescribeDataset",
          "personalize:DescribeDatasetGroup",
          "personalize:DescribeDatasetImportJob",
          "personalize:DescribeEventTracker",
          "personalize:DescribeRecommender",
          "personalize:DescribeSchema",
          "personalize:GetRecommendations",
          "personalize:ListDatasetGroups",
          "personalize:ListDatasets",
          "personalize:ListDatasetImportJobs",
          "personalize:ListEventTrackers",
          "personalize:ListRecommenders",
          "personalize:ListSchemas",
          "personalize:PutEvents",
          "personalize:UpdateDataset",
          "personalize:UpdateDatasetGroup",
          "personalize:UpdateEventTracker",
          "personalize:UpdateRecommender"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_prefix}-personalize",
          "arn:aws:s3:::${var.s3_bucket_prefix}-personalize/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "personalize_policy_attachment" {
  role       = aws_iam_role.personalize_role.name
  policy_arn = aws_iam_policy.personalize_policy.arn
} 