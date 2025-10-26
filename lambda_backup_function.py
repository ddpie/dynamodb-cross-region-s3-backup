import boto3
import json
from datetime import datetime
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # 配置参数
    table_arns = os.environ['TABLE_ARNS'].split(',')  # 逗号分隔的表ARN列表
    s3_bucket = os.environ['S3_BUCKET']
    s3_region = os.environ.get('S3_REGION', 'us-west-2')
    source_region = os.environ.get('SOURCE_REGION', 'us-east-1')
    
    if not table_arns or table_arns[0].strip() == '':
        logger.error("TABLE_ARNS环境变量为空")
        return {
            'statusCode': 400,
            'body': json.dumps({'success': False, 'error': 'TABLE_ARNS is empty'})
        }
    
    dynamodb = boto3.client('dynamodb', region_name=source_region)
    s3 = boto3.client('s3', region_name=s3_region)
    
    results = []
    current_time = datetime.now()
    
    for table_arn in table_arns:
        table_arn = table_arn.strip()
        table_name = table_arn.split('/')[-1]
        
        try:
            s3_prefix = f'daily/{current_time.strftime("%Y/%m/%d")}/{table_name}'
            
            logger.info(f"开始备份: {table_arn}")
            logger.info(f"目标S3: s3://{s3_bucket}/{s3_prefix}")
            
            # 1. 导出表结构
            table_info = dynamodb.describe_table(TableName=table_name)
            table = table_info['Table']
            
            schema = {
                'TableName': table['TableName'],
                'AttributeDefinitions': table['AttributeDefinitions'],
                'KeySchema': table['KeySchema'],
                'BillingMode': table.get('BillingModeSummary', {}).get('BillingMode', 'PAY_PER_REQUEST')
            }
            
            if 'GlobalSecondaryIndexes' in table:
                schema['GlobalSecondaryIndexes'] = [
                    {
                        'IndexName': gsi['IndexName'],
                        'KeySchema': gsi['KeySchema'],
                        'Projection': gsi['Projection']
                    }
                    for gsi in table['GlobalSecondaryIndexes']
                ]
            
            if 'LocalSecondaryIndexes' in table:
                schema['LocalSecondaryIndexes'] = [
                    {
                        'IndexName': lsi['IndexName'],
                        'KeySchema': lsi['KeySchema'],
                        'Projection': lsi['Projection']
                    }
                    for lsi in table['LocalSecondaryIndexes']
                ]
            
            # 保存表结构到S3
            schema_key = f'{s3_prefix}/table_schema.json'
            s3.put_object(
                Bucket=s3_bucket,
                Key=schema_key,
                Body=json.dumps(schema, indent=2),
                ContentType='application/json'
            )
            logger.info(f"表结构已保存: s3://{s3_bucket}/{schema_key}")
            
            # 2. 导出表数据
            response = dynamodb.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=s3_bucket,
                S3Prefix=s3_prefix,
                ExportFormat='DYNAMODB_JSON',
                S3SseAlgorithm='AES256'
            )
            
            export_arn = response['ExportDescription']['ExportArn']
            export_status = response['ExportDescription']['ExportStatus']
            
            logger.info(f"数据导出任务已创建: {export_arn}")
            logger.info(f"初始状态: {export_status}")
            
            results.append({
                'table_name': table_name,
                'success': True,
                'export_arn': export_arn,
                'status': export_status
            })
            
        except Exception as e:
            logger.error(f"备份表 {table_name} 失败: {str(e)}")
            results.append({
                'table_name': table_name,
                'success': False,
                'error': str(e)
            })
    
    success_count = sum(1 for r in results if r['success'])
    
    return {
        'statusCode': 200 if success_count == len(table_arns) else 207,
        'body': json.dumps({
            'backup_date': current_time.strftime("%Y-%m-%d"),
            'total_tables': len(table_arns),
            'success_count': success_count,
            'results': results
        })
    }
