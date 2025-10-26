#!/usr/bin/env python3
import boto3
import json
import sys

def export_schema(table_name, source_region='us-east-1'):
    """导出DynamoDB表结构到table_schema.json"""
    dynamodb = boto3.client('dynamodb', region_name=source_region)
    
    try:
        response = dynamodb.describe_table(TableName=table_name)
        table = response['Table']
        
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
        
        with open('table_schema.json', 'w') as f:
            json.dump(schema, f, indent=2)
        
        print(f"表结构已导出到 table_schema.json")
        print(f"\n表信息:")
        print(f"  表名: {schema['TableName']}")
        print(f"  主键: {schema['KeySchema']}")
        print(f"  计费模式: {schema['BillingMode']}")
        if 'GlobalSecondaryIndexes' in schema:
            print(f"  GSI数量: {len(schema['GlobalSecondaryIndexes'])}")
        
        return schema
        
    except Exception as e:
        print(f"导出失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python export_table_schema.py TABLE_NAME [REGION]")
        print("示例: python export_table_schema.py MyTable us-east-1")
        sys.exit(1)
    
    table_name = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) > 2 else 'us-east-1'
    
    export_schema(table_name, region)
