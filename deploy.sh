#!/bin/bash
set -e

# 配置变量
FUNCTION_NAME="DynamoDBBackupFunction"
ROLE_NAME="DynamoDBBackupRole"
# 多表配置：逗号分隔的表ARN列表
TABLE_ARNS="arn:aws:dynamodb:us-east-1:YOUR_ACCOUNT:table/YOUR_TABLE_1,arn:aws:dynamodb:us-east-1:YOUR_ACCOUNT:table/YOUR_TABLE_2"
# 用于权限配置的表名列表（空格分隔）
TABLE_NAMES="YOUR_TABLE_1 YOUR_TABLE_2"
S3_BUCKET="your-backup-bucket-name"
ACCOUNT_ID="YOUR_ACCOUNT"
TABLE_REGION="us-east-1"
S3_REGION="us-west-2"

echo "开始部署DynamoDB备份方案..."

# 前置条件检查
echo "检查前置条件..."

# 检查配置是否修改
if [[ "$ACCOUNT_ID" == "YOUR_ACCOUNT" ]] || [[ "$TABLE_ARNS" == *"YOUR_TABLE"* ]] || [[ "$S3_BUCKET" == "your-backup-bucket-name" ]]; then
    echo "错误: 请先修改脚本中的配置变量"
    exit 1
fi

# 检查AWS CLI
if ! command -v aws &> /dev/null; then
    echo "错误: 未安装AWS CLI"
    exit 1
fi

# 检查AWS凭证
if ! aws sts get-caller-identity &> /dev/null; then
    echo "错误: AWS凭证未配置或已过期"
    exit 1
fi

# 检查DynamoDB表是否存在
echo "检查DynamoDB表..."
for TABLE_NAME in $TABLE_NAMES; do
    if ! aws dynamodb describe-table --table-name $TABLE_NAME --region $TABLE_REGION &> /dev/null; then
        echo "错误: DynamoDB表 $TABLE_NAME 不存在"
        exit 1
    fi
    
    # 检查PITR是否启用
    echo "检查表 $TABLE_NAME 的PITR状态..."
    PITR_STATUS=$(aws dynamodb describe-continuous-backups --table-name $TABLE_NAME --region $TABLE_REGION --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus' --output text)
    if [[ "$PITR_STATUS" != "ENABLED" ]]; then
        echo "错误: 表 $TABLE_NAME 未启用PITR，请先启用"
        echo "运行: aws dynamodb update-continuous-backups --table-name $TABLE_NAME --point-in-time-recovery-specification PointInTimeRecoveryEnabled=True --region $TABLE_REGION"
        exit 1
    fi
done

# 检查S3存储桶是否存在
echo "检查S3存储桶..."
if ! aws s3api head-bucket --bucket $S3_BUCKET --region $S3_REGION 2> /dev/null; then
    echo "错误: S3存储桶 $S3_BUCKET 不存在"
    exit 1
fi

# 检查lambda_backup_function.py是否存在
if [[ ! -f "lambda_backup_function.py" ]]; then
    echo "错误: lambda_backup_function.py 文件不存在"
    exit 1
fi

echo "前置条件检查通过"

# 1. 创建IAM角色
echo "创建IAM角色..."
aws iam create-role \
    --role-name $ROLE_NAME \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }'

# 2. 附加策略
echo "附加IAM策略..."
aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# 构建表ARN列表用于IAM策略
TABLE_ARN_LIST=""
for TABLE_NAME in $TABLE_NAMES; do
    TABLE_ARN_LIST="$TABLE_ARN_LIST\"arn:aws:dynamodb:$TABLE_REGION:$ACCOUNT_ID:table/$TABLE_NAME\","
done
TABLE_ARN_LIST=${TABLE_ARN_LIST%,}  # 移除最后的逗号

aws iam put-role-policy \
    --role-name $ROLE_NAME \
    --policy-name DynamoDBBackupPolicy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:ExportTableToPointInTime",
                    "dynamodb:DescribeExport",
                    "dynamodb:ListExports",
                    "dynamodb:DescribeTable",
                    "dynamodb:DescribeContinuousBackups",
                    "dynamodb:ImportTable",
                    "dynamodb:DescribeImport",
                    "dynamodb:ListImports"
                ],
                "Resource": ['$TABLE_ARN_LIST']
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:GetBucketLocation",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::'$S3_BUCKET'",
                    "arn:aws:s3:::'$S3_BUCKET'/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                    "logs:PutLogEvents",
                    "logs:PutRetentionPolicy"
                ],
                "Resource": "arn:aws:logs:'$TABLE_REGION':'$ACCOUNT_ID':log-group:/aws-dynamodb/*"
            }
        ]
    }'

# 等待IAM角色生效
echo "等待IAM角色生效..."
MAX_WAIT=60
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    if aws iam get-role --role-name $ROLE_NAME &> /dev/null; then
        echo "IAM角色已生效"
        sleep 5
        break
    fi
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))
done

if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
    echo "警告: IAM角色等待超时，继续部署可能失败"
fi

# 3. 创建部署包
echo "创建部署包..."
zip function.zip lambda_backup_function.py

# 4. 创建Lambda函数
echo "创建Lambda函数..."
aws lambda create-function \
    --function-name $FUNCTION_NAME \
    --runtime python3.9 \
    --role arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME \
    --handler lambda_backup_function.lambda_handler \
    --zip-file fileb://function.zip \
    --timeout 900 \
    --environment Variables="{TABLE_ARNS=$TABLE_ARNS,S3_BUCKET=$S3_BUCKET,S3_REGION=$S3_REGION}"

# 5. 创建每日备份调度
echo "创建每日备份调度..."
aws events put-rule \
    --name daily-backup \
    --schedule-expression 'cron(0 2 * * ? *)'

aws lambda add-permission \
    --function-name $FUNCTION_NAME \
    --statement-id daily-backup \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn arn:aws:events:us-east-1:$ACCOUNT_ID:rule/daily-backup

aws events put-targets \
    --rule daily-backup \
    --targets '[{
        "Id": "1",
        "Arn": "arn:aws:lambda:us-east-1:'$ACCOUNT_ID':function:'$FUNCTION_NAME'"
    }]'

echo "部署完成！"

# 验证部署
echo "验证部署..."
if aws lambda get-function --function-name $FUNCTION_NAME --region $TABLE_REGION &> /dev/null; then
    echo "✓ Lambda函数创建成功"
else
    echo "✗ Lambda函数创建失败"
fi

if aws events describe-rule --name daily-backup --region $TABLE_REGION &> /dev/null; then
    echo "✓ EventBridge规则创建成功"
else
    echo "✗ EventBridge规则创建失败"
fi

echo ""
echo "备份配置："
echo "  表数量: $(echo $TABLE_NAMES | wc -w)"
echo "  表列表: $TABLE_NAMES"
echo "  备份时间: 每天凌晨2点(UTC)"
echo "  存储位置: s3://$S3_BUCKET/daily/"
echo ""
echo "下一步："
echo "1. 配置S3生命周期策略: aws s3api put-bucket-lifecycle-configuration --bucket $S3_BUCKET --lifecycle-configuration file://s3-lifecycle.json"
echo "2. 测试备份: python monitor.py backup"
echo "3. 查看日志: aws logs tail /aws/lambda/$FUNCTION_NAME --follow"
