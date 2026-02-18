"""
Lambda handler for database import operations (CSV/XLSX files).

This module provides functionality for bulk lead import with:
- Pre-signed S3 URL generation for secure file uploads
- CSV/XLSX parsing and validation using pandas
- Duplicate phone number detection and skipping
- Async batch processing of lead creation
- Import status tracking with polling support
- Results file generation with import outcomes

Features:
- Supports up to 5,000 leads per import
- Automatic phone normalization and validation
- Concurrent import prevention per company
- 30-day auto-cleanup of import history
- Portuguese error messages for Brazilian users
"""

import json
import os
import re
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
from auris_tools.databaseHandlers import DatabaseHandler
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

from src.shared.settings import Settings
from src.shared.utils import normalize_phone, response

logger = Logger(service='gl-database-import')
settings = Settings()

# S3 Configuration
UPLOAD_BUCKET = 'auris-database-imports'
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
MAX_ROWS = 5000
ALLOWED_EXTENSIONS = ['.csv', '.xlsx']
PRESIGNED_URL_EXPIRATION = 3600  # 1 hour

# Processing Configuration
BATCH_SIZE = 10  # Messages per SQS batch (max 10)
BATCH_UPDATE_INTERVAL = 3  # Update status every N batches
TIMEOUT_MINUTES = 3  # Mark as timeout after this duration

# Required CSV columns (exact match)
REQUIRED_COLUMNS = ['fullName', 'phone', 'source', 'entryDate']

# CORS Headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,x-api-key,X-Amz-Date,Authorization,X-Api-Key',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
    'Access-Control-Max-Age': '3600',
}


def generate_presigned_upload_url(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    """
    Generate pre-signed S3 URL for secure file upload and initialize import record.

    This handler creates a temporary upload URL that allows the frontend to upload
    files directly to S3. It also validates concurrent import restrictions and
    initializes the import status tracking in DynamoDB.

    Process:
    1. Validate request payload (fileName, fileSize, fileType, companyID, etc.)
    2. Check file extension (.csv or .xlsx) and size (≤100MB)
    3. Query for active imports to prevent concurrent uploads
    4. Generate unique importID and S3 key
    5. Create import-status record in DynamoDB with TTL
    6. Generate pre-signed POST URL with security constraints
    7. Return URL and importID for frontend tracking

    Args:
        event: API Gateway event with body containing upload metadata
        context: Lambda context

    Expected Request Body:
        {
            "fileName": "leads_import.csv",
            "fileSize": 52428800,
            "fileType": "text/csv",
            "companyID": "comp-123",
            "assignedUser": "user@example.com"
        }

    Response (200 OK):
        {
            "uploadUrl": "https://s3.amazonaws.com/...",
            "fields": {...},
            "importID": "import-uuid",
            "expiresIn": 3600
        }

    Error Responses:
        400 - Invalid file format/size or missing fields
        409 - Active import already in progress for company
        500 - Internal server error
    """
    try:
        logger.info('Gerando URL de upload pré-assinada')

        # Handle OPTIONS preflight
        if event.get('httpMethod') == 'OPTIONS':
            return response(status_code=200, message='', headers=CORS_HEADERS)

        # Extract authenticated user email from Cognito authorizer
        try:
            authorizer = event.get('requestContext', {}).get('authorizer', {})
            claims = authorizer.get('claims', {})
            authenticated_email = claims.get('email', '').strip().lower()

            if not authenticated_email:
                logger.error('Missing email in Cognito token')
                return response(
                    status_code=401,
                    message={
                        'error': 'Authentication error: email claim missing in token'
                    },
                    headers=CORS_HEADERS,
                )

            logger.info(f'Request authenticated for user: {authenticated_email}')
        except Exception as e:
            logger.error(f'Failed to extract authenticated user: {str(e)}')
            return response(
                status_code=401,
                message={
                    'error': 'Authentication error: unable to verify user identity'
                },
                headers=CORS_HEADERS,
            )

        # Extract and validate payload
        body = json.loads(event.get('body', '{}'))

        file_name = body.get('fileName', '').strip()
        file_size = body.get('fileSize', 0)
        file_type = body.get('fileType', '').strip()
        company_id = body.get('companyID', '').strip()
        assigned_user = body.get('assignedUser', '').strip()

        # Validate required fields
        if not file_name:
            return response(
                status_code=400,
                message={'error': 'Nome do arquivo é obrigatório'},
                headers=CORS_HEADERS,
            )

        if not company_id:
            return response(
                status_code=400,
                message={'error': 'ID da empresa é obrigatório'},
                headers=CORS_HEADERS,
            )

        user_email = authenticated_email

        # Validate file extension
        file_ext = Path(file_name).suffix.lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            return response(
                status_code=400,
                message={'error': f'Formato de arquivo inválido. Use .csv ou .xlsx'},
                headers=CORS_HEADERS,
            )

        # Validate file size
        if file_size <= 0:
            return response(
                status_code=400,
                message={'error': 'Tamanho do arquivo inválido'},
                headers=CORS_HEADERS,
            )

        if file_size > MAX_FILE_SIZE:
            return response(
                status_code=400,
                message={
                    'error': f'Arquivo excede o tamanho máximo de {MAX_FILE_SIZE / (1024*1024):.0f}MB'
                },
                headers=CORS_HEADERS,
            )

        # Check for concurrent imports
        import_db = DatabaseHandler(table_name=settings.import_status_table_name)

        # try:
        #     # Query GSI for active imports using boto3 client
        #     query_response = import_db.client.query(
        #         TableName=settings.import_status_table_name,
        #         IndexName='companyID-createdAt-index',
        #         KeyConditionExpression='companyID = :cid',
        #         ExpressionAttributeValues={':cid': {'S': company_id}},
        #     )

        #     # Deserialize items from DynamoDB format
        #     active_imports_raw = query_response.get('Items', [])
        #     active_imports = (
        #         [import_db._deserialize_item(item) for item in active_imports_raw]
        #         if active_imports_raw
        #         else []
        #     )

        #     # Filter for pending or processing status
        #     active_count = sum(
        #         1
        #         for item in active_imports
        #         if item.get('status') in ['pending', 'processing']
        #     )

        #     if active_count > 0:
        #         logger.warning(f'Importação ativa já em andamento para empresa: {company_id} com atividades: {active_imports}')
        #         return response(
        #             status_code=409,
        #             message={
        #                 'error': 'Uma importação já está em andamento para esta empresa'
        #             },
        #             headers=CORS_HEADERS,
        #         )
        # except Exception as e:
        #     logger.warning(f'Erro ao verificar importações ativas: {str(e)}')
        #     # Continue if GSI not available yet

        # Generate unique import ID and S3 key
        import_id = f'import-{str(uuid.uuid4())}'
        date_prefix = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        s3_key = f'uploads/{settings.stage}/{company_id}/{date_prefix}/{import_id}_file_{file_name}'

        # Calculate TTL (30 days from now)
        ttl_timestamp = int(
            (datetime.now(timezone.utc) + timedelta(days=30)).timestamp()
        )

        # Create import status record
        current_timestamp = datetime.now(timezone.utc).isoformat()
        import_record = {
            'importID': import_id,
            'companyID': company_id,
            'userEmail': user_email,
            'assignedUser': assigned_user,
            'fileName': file_name,
            'fileSize': file_size,
            's3Key': s3_key,
            'status': 'pending',
            'totalRows': 0,
            'processedCount': 0,
            'successCount': 0,
            'failedCount': 0,
            'duplicateCount': 0,
            'createdAt': current_timestamp,
            'updatedAt': current_timestamp,
            'expiresAt': ttl_timestamp,
        }

        import_db.insert_item(item=import_record, primary_key='importID')

        logger.info(f'Registro de importação criado: {import_id}')

        # Generate pre-signed POST URL
        s3_client = boto3.client('s3', region_name=settings.region)

        presigned_post = s3_client.generate_presigned_post(
            Bucket=UPLOAD_BUCKET,
            Key=s3_key,
            Fields={
                'Content-Type': file_type,
                'x-amz-meta-import-id': import_id,
                'x-amz-meta-company-id': company_id,
                'x-amz-meta-user-email': user_email,
            },
            Conditions=[
                {'Content-Type': file_type},
                ['content-length-range', 1, MAX_FILE_SIZE],
                {'x-amz-meta-import-id': import_id},
                {'x-amz-meta-company-id': company_id},
            ],
            ExpiresIn=PRESIGNED_URL_EXPIRATION,
        )

        logger.info(f'URL pré-assinada gerada para {s3_key}')

        return response(
            status_code=200,
            message={
                'uploadUrl': presigned_post['url'],
                'fields': presigned_post['fields'],
                'importID': import_id,
                'expiresIn': PRESIGNED_URL_EXPIRATION,
                'message': 'URL de upload gerada com sucesso',
            },
            headers=CORS_HEADERS,
        )

    except ValueError as e:
        logger.warning(f'Erro de validação: {str(e)}')
        return response(
            status_code=400, message={'error': str(e)}, headers=CORS_HEADERS
        )
    except Exception as e:
        logger.error(f'Erro ao gerar URL pré-assinada: {str(e)}', exc_info=True)
        return response(
            status_code=500,
            message={'error': 'Falha ao gerar URL de upload'},
            headers=CORS_HEADERS,
        )


def database_import_orchestrator(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Orchestrate the complete database import process triggered by S3 upload.

    This handler is triggered automatically when a file is uploaded to S3.
    It coordinates the entire import workflow from validation to completion.

    Process:
    1. Extract importID from S3 object key
    2. Update status to 'processing'
    3. Download file from S3 to /tmp/
    4. Parse and validate file with pandas
    5. Filter duplicate phone numbers
    6. Process leads asynchronously in batches
    7. Generate results file with outcomes
    8. Move original file to processed/ folder
    9. Update status to 'completed'

    S3 Event Structure:
        {
            "Records": [{
                "s3": {
                    "bucket": {"name": "auris-database-imports"},
                    "object": {"key": "uploads/comp-123/2026-01-27/import-uuid-file.csv"}
                }
            }]
        }

    Args:
        event: S3 event notification
        context: Lambda context

    Returns:
        Success/failure status with import summary
    """
    s3_client = boto3.client('s3', region_name=settings.region)
    import_db = DatabaseHandler(table_name=settings.import_status_table_name)

    import_id = None
    bucket_name = None
    object_key = None

    try:
        logger.info(f'Processando evento S3: {json.dumps(event)}')

        # Extract S3 information
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        # Extract importID from key pattern: uploads/{stage}/{companyID}/{date}/{importID}_file_{fileName}
        import_id_match = re.search(r'/(import-[a-f0-9-]+)_file_', object_key)
        if not import_id_match:
            raise ValueError(
                f'ID de importação não encontrado na chave S3: {object_key}'
            )

        import_id = import_id_match.group(1)
        logger.info(f'Processando importação: {import_id}')

        # Retrieve import metadata
        import_record_raw = import_db.get_item(key={'importID': import_id})
        if not import_record_raw:
            raise ValueError(f'Registro de importação não encontrado: {import_id}')

        # Deserialize DynamoDB item
        import_record = import_db._deserialize_item(import_record_raw)

        company_id = import_record['companyID']
        user_email = import_record['userEmail']
        assigned_user = import_record.get('assignedUser', user_email)
        file_name = import_record['fileName']

        # Update status to processing
        current_timestamp = datetime.now(timezone.utc).isoformat()
        import_db.update_item(
            key={'importID': import_id},
            updates={'status': 'processing', 'updatedAt': current_timestamp},
            primary_key='importID',
        )

        logger.info(f'Status atualizado para processing: {import_id}')

        # Download file from S3
        local_file_path = f'/tmp/{import_id}-{file_name}'
        s3_client.download_file(bucket_name, object_key, local_file_path)
        logger.info(f'Arquivo baixado: {local_file_path}')

        # Determine file type
        file_ext = Path(file_name).suffix.lower()

        # Step 1: Parse and validate file
        complete_df, validated_df, validation_errors = _parse_and_validate_file(
            file_path=local_file_path,
            file_type=file_ext,
            import_id=import_id,
            import_db=import_db,
        )

        logger.info(
            f'Validação concluída - Válidos: {len(validated_df)}, '
            f'Erros: {len(validation_errors)}'
        )

        # Step 2: Filter duplicates
        filtered_df, duplicate_count = _filter_duplicate_leads(
            complete_df=complete_df, validated_df=validated_df, company_id=company_id
        )

        logger.info(
            f'Filtro de duplicatas concluído - '
            f'Únicos: {len(filtered_df)}, Duplicados: {duplicate_count}'
        )

        # Step 3: Process leads via SQS queue
        total_queued, queue_failures = _process_leads_async(
            df=filtered_df,
            company_id=company_id,
            assigned_user=assigned_user,
            user_email=user_email,
            import_id=import_id,
            import_db=import_db,
        )

        logger.info(
            f'Processamento na fila concluído - '
            f'Na fila: {total_queued}, Falhas: {queue_failures}'
        )

        # Step 4: Generate results file
        results_file_key = _generate_results_file(
            complete_df=complete_df,
            company_id=company_id,
            import_id=import_id,
            s3_client=s3_client,
        )

        logger.info(f'Arquivo de resultados gerado: {results_file_key}')

        # Step 5: Finalize import
        completion_timestamp = datetime.now(timezone.utc).isoformat()
        date_prefix = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Move original file to processed folder
        processed_key = f'processed/{settings.stage}/{company_id}/{date_prefix}/{import_id}-{file_name}'
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': object_key},
            Key=processed_key,
        )

        # Skip deletion in local debug mode
        if os.environ.get('AWS_EXECUTION_ENV'):
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        else:
            logger.info(f'Modo local: pulando deleção de {object_key}')

        logger.info(f'Arquivo movido para: {processed_key}')

        # Update final status
        import_db.update_item(
            key={'importID': import_id},
            updates={
                'status': 'completed',
                'completedAt': completion_timestamp,
                'resultsFileKey': results_file_key,
                'failedCount': len(validation_errors),
                'duplicateCount': duplicate_count,
                'updatedAt': completion_timestamp,
            },
            primary_key='importID',
        )

        logger.info(f'Importação concluída com sucesso: {import_id}')

        # Cleanup temp file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

        return response(
            status_code=200,
            message={
                'message': 'Importação concluída com sucesso',
                'importID': import_id,
                'totalRows': int(import_record.get('totalRows', 0)),
                'queuedCount': total_queued,
                'failedCount': len(validation_errors),
                'duplicateCount': duplicate_count,
            },
        )

    except Exception as e:
        error_msg = f'Erro no processamento da importação: {str(e)}'
        logger.error(error_msg, exc_info=True)

        # Update status to failed
        if import_id:
            try:
                import_db.update_item(
                    key={'importID': import_id},
                    updates={
                        'status': 'failed',
                        'errorMessage': str(e),
                        'updatedAt': datetime.now(timezone.utc).isoformat(),
                    },
                    primary_key='importID',
                )

                # Move file to failed folder
                if bucket_name and object_key:
                    date_prefix = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                    failed_key = object_key.replace('uploads/', 'failed/')
                    s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': object_key},
                        Key=failed_key,
                    )

                    # Skip deletion in local debug mode
                    if os.environ.get('AWS_EXECUTION_ENV'):
                        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
                    else:
                        logger.info(f'Modo local: pulando deleção de {object_key}')

            except Exception as cleanup_error:
                logger.error(f'Erro na limpeza: {str(cleanup_error)}')

        return response(status_code=500, message={'error': error_msg})


def _parse_and_validate_file(
    file_path: str, file_type: str, import_id: str, import_db: DatabaseHandler
) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
    """
    Parse CSV/XLSX file and validate data against schema requirements.

    Process:
    1. Read file using pandas (auto-detect encoding for CSV)
    2. Validate row count ≤5,000
    3. Validate required columns exist
    4. Validate required fields in each row
    5. Normalize phone numbers
    6. Collect validation errors

    Args:
        file_path: Path to downloaded file in /tmp/
        file_type: File extension (.csv or .xlsx)
        import_id: Import identifier for tracking
        import_db: DatabaseHandler for import-status table

    Returns:
        Tuple of (complete_dataframe, validated_dataframe, validation_errors_list)

    Raises:
        ValueError: If file exceeds row limit or missing required columns
    """
    logger.info(f'Analisando arquivo: {file_path}')

    # Read file with pandas
    try:
        if file_type == '.csv':
            df = pd.read_csv(file_path, encoding=None)  # Auto-detect encoding
        elif file_type == '.xlsx':
            df = pd.read_excel(file_path, engine='openpyxl')
        else:
            raise ValueError(f'Tipo de arquivo não suportado: {file_type}')
    except Exception as e:
        raise ValueError(f'Erro ao ler arquivo: {str(e)}')

    # Validate row count
    row_count = len(df)
    if row_count > MAX_ROWS:
        raise ValueError(f'Arquivo excede o máximo de {MAX_ROWS} linhas')

    logger.info(f'Linhas lidas: {row_count}')

    # Update total rows in DynamoDB
    import_db.update_item(
        key={'importID': import_id},
        updates={'totalRows': row_count},
        primary_key='importID',
    )

    # Validate required columns
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(f'Colunas obrigatórias ausentes: {", ".join(missing_columns)}')

    # Add validation_error_message column to track errors
    df['validation_error_message'] = ''

    # Validate and normalize each row
    validation_errors = []

    for idx, row in df.iterrows():
        row_number = idx + 2  # +2 because: 0-indexed + 1 for header row
        row_errors = []

        # Validate required fields are non-empty
        for field in REQUIRED_COLUMNS:
            value = row.get(field)

            # Check if null or empty after stripping
            if pd.isna(value) or (isinstance(value, str) and not value.strip()):
                error_msg = 'Campo obrigatório vazio'
                validation_errors.append(
                    {
                        'row': row_number,
                        'field': field,
                        'value': str(value) if not pd.isna(value) else 'NULL',
                        'error': error_msg,
                    }
                )
                row_errors.append(f'{field}: {error_msg}')

        # Normalize phone number
        if 'phone' in row and not pd.isna(row['phone']):
            try:
                normalized = normalize_phone(str(row['phone']))
                df.at[idx, 'phone'] = normalized
            except ValueError as e:
                error_msg = 'Formato de telefone inválido'
                validation_errors.append(
                    {
                        'row': row_number,
                        'field': 'phone',
                        'value': str(row['phone']),
                        'error': error_msg,
                    }
                )
                row_errors.append(f'phone: {error_msg}')
                # Mark phone as invalid so it won't be processed
                df.at[idx, 'phone'] = None

        # Store error message in DataFrame (truncate to 100 chars)
        if row_errors:
            full_error = '; '.join(row_errors)
            df.at[idx, 'validation_error_message'] = full_error[:100]

    logger.info(f'Validação concluída - {len(validation_errors)} erros encontrados')

    # Keep complete DataFrame for results generation
    complete_df = df.copy()

    # Create validated DataFrame with only valid rows
    valid_mask = df[REQUIRED_COLUMNS].notna().all(axis=1)
    validated_df = df[valid_mask].copy()

    return complete_df, validated_df, validation_errors


def _filter_duplicate_leads(
    complete_df: pd.DataFrame, validated_df: pd.DataFrame, company_id: str
) -> Tuple[pd.DataFrame, int]:
    """
    Filter out leads with duplicate phone numbers by querying existing leads.

    Uses the companyID-phone-index GSI to efficiently check for existing
    phone numbers in the leads table.

    Process:
    1. Extract all normalized phone numbers from validated DataFrame
    2. Query DynamoDB GSI for each unique phone
    3. Build set of duplicate phones
    4. Mark duplicate rows in complete DataFrame
    5. Filter to keep only non-duplicates

    Args:
        complete_df: Complete DataFrame with all rows including validation errors
        validated_df: Validated DataFrame with normalized phone numbers
        company_id: Company identifier for scoped duplicate check

    Returns:
        Tuple of (filtered_dataframe, duplicate_count)
    """
    logger.info(f'Verificando duplicatas para empresa: {company_id}')

    # Add is_duplicate column to complete DataFrame
    complete_df['is_duplicate'] = False

    # Extract unique phones from validated data
    unique_phones = validated_df['phone'].dropna().unique()

    if len(unique_phones) == 0:
        logger.info('Nenhum telefone para verificar duplicatas')
        return validated_df, 0

    logger.info(f'Verificando {len(unique_phones)} números únicos')

    # Initialize leads database handler
    leads_db = DatabaseHandler(table_name=settings.leads_table_name)

    # Query for duplicates
    duplicate_phones = set()

    for phone in unique_phones:
        try:
            # Query GSI using boto3 client
            response = leads_db.client.query(
                TableName=settings.leads_table_name,
                IndexName='companyID-phone-index',
                KeyConditionExpression='companyID = :cid AND phone = :phone',
                ExpressionAttributeValues={
                    ':cid': {'S': company_id},
                    ':phone': {'S': phone},
                },
            )

            # Deserialize items from DynamoDB format
            results_raw = response.get('Items', [])
            results = (
                [leads_db._deserialize_item(item) for item in results_raw]
                if results_raw
                else []
            )

            if results and len(results) > 0:
                duplicate_phones.add(phone)
                logger.debug(f'Duplicata encontrada: {phone}')

        except Exception as e:
            logger.warning(f'Erro ao consultar telefone {phone}: {str(e)}')
            # Continue with other phones

    logger.info(f'Duplicatas encontradas: {len(duplicate_phones)}')

    # Mark duplicates in complete DataFrame
    complete_df['is_duplicate'] = complete_df['phone'].isin(duplicate_phones)

    # Filter validated DataFrame to keep only non-duplicates
    filtered_df = validated_df[~validated_df['phone'].isin(duplicate_phones)].copy()
    duplicate_count = len(validated_df[validated_df['phone'].isin(duplicate_phones)])

    logger.info(f'Leads únicos para importar: {len(filtered_df)}')

    return filtered_df, duplicate_count


def _process_leads_async(
    df: pd.DataFrame,
    company_id: str,
    assigned_user: str,
    user_email: str,
    import_id: str,
    import_db: DatabaseHandler,
) -> Tuple[int, int]:
    """
    Process validated leads by sending messages to SQS queue.

    Splits leads into batches and sends messages to the operations queue
    for async processing via gl_queue_manager. Updates progress periodically.

    Process:
    1. Split DataFrame into chunks of BATCH_SIZE (max 10 for SQS)
    2. For each lead, construct message payload with importID
    3. Use send_message_batch for efficient queue operations
    4. Track successful queued messages and failures
    5. Update progress in DynamoDB every N batches

    Args:
        df: Filtered DataFrame with non-duplicate leads
        company_id: Company identifier
        assigned_user: User to assign leads to
        user_email: Email of user performing import
        import_id: Import identifier for tracking
        import_db: DatabaseHandler for import-status table

    Returns:
        Tuple of (total_queued, queue_failures)
    """
    logger.info(f'Enviando {len(df)} leads para a fila')

    if len(df) == 0:
        return 0, 0

    # Initialize SQS client
    sqs_client = boto3.client('sqs', region_name=settings.region)
    queue_url = settings.operations_queue_url

    if not queue_url:
        raise ValueError('OPERATIONS_QUEUE_URL environment variable not set')

    total_queued = 0
    queue_failures = 0

    # Split into chunks (max 10 messages per SQS batch)
    chunks = [df.iloc[i : i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]

    logger.info(f'Processando em {len(chunks)} lotes de até {BATCH_SIZE} mensagens')

    for chunk_idx, chunk in enumerate(chunks, start=1):
        # Prepare batch entries for SQS
        batch_entries = []

        for idx, row in chunk.iterrows():
            # Build lead payload
            lead_data = {
                'fullName': str(row.get('fullName', '')),
                'phone': str(row.get('phone', '')),
                'city': str(row.get('city', '')),
                'source': str(row.get('source', '')),
                'companyID': company_id,
                'assignedUser': assigned_user,
                'importID': import_id,  # Add importID for tracking
            }

            # Add optional fields if present
            if 'email' in row and not pd.isna(row['email']):
                lead_data['email'] = str(row['email'])

            if 'entryDate' in row and not pd.isna(row['entryDate']):
                try:
                    # Parse date and convert to ISO format timestamp
                    entry_date = pd.to_datetime(row['entryDate'])
                    lead_data['entryDate'] = entry_date.isoformat()
                except Exception as e:
                    logger.warning(f'Erro ao converter entryDate: {str(e)}')
                    lead_data['entryDate'] = str(row['entryDate'])

            if 'allowsMarketing' in row and not pd.isna(row['allowsMarketing']):
                lead_data['allowsMarketing'] = bool(row['allowsMarketing'])

            if 'statusLead' in row and not pd.isna(row['statusLead']):
                lead_data['statusLead'] = str(row['statusLead'])

            if 'statusClassification' in row and not pd.isna(
                row['statusClassification']
            ):
                lead_data['statusClassification'] = str(row['statusClassification'])

            if 'audiologist' in row and not pd.isna(row['audiologist']):
                lead_data['audiologist'] = str(row['audiologist'])

            if 'reminderDate' in row and not pd.isna(row['reminderDate']):
                try:
                    # Parse date and convert to ISO format timestamp
                    reminder_date = pd.to_datetime(row['reminderDate'])
                    lead_data['reminderDate'] = reminder_date.isoformat()
                except Exception as e:
                    logger.warning(f'Erro ao converter reminderDate: {str(e)}')
                    lead_data['reminderDate'] = str(row['reminderDate'])

            # Construct SQS message body
            message_body = {
                'operationType': 'add_new_lead',
                'payload': lead_data,
                'userEmail': user_email,
                'companyID': company_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'invocationType': 'Event',
            }

            # Add to batch
            batch_entries.append(
                {'Id': str(uuid.uuid4()), 'MessageBody': json.dumps(message_body)}
            )

        # Send batch to SQS
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url, Entries=batch_entries
            )

            successful = len(response.get('Successful', []))
            failed = len(response.get('Failed', []))

            total_queued += successful
            queue_failures += failed

            if failed > 0:
                logger.warning(f'Falhas no lote {chunk_idx}: {response.get("Failed")}')

            logger.info(
                f'Lote {chunk_idx}/{len(chunks)} enviado - {successful} mensagens na fila'
            )

        except Exception as e:
            logger.error(f'Erro ao enviar lote para SQS: {str(e)}')
            queue_failures += len(batch_entries)

        # Update progress every N batches
        if chunk_idx % BATCH_UPDATE_INTERVAL == 0 or chunk_idx == len(chunks):
            try:
                import_db.update_item(
                    key={'importID': import_id},
                    updates={
                        'processedCount': total_queued,
                        'queuedCount': total_queued,
                        'updatedAt': datetime.now(timezone.utc).isoformat(),
                    },
                    primary_key='importID',
                )
            except Exception as e:
                logger.warning(f'Erro ao atualizar progresso: {str(e)}')

    logger.info(
        f'Processamento concluído - '
        f'Na fila: {total_queued}, Falhas: {queue_failures}'
    )

    return total_queued, queue_failures


def _generate_results_file(
    complete_df: pd.DataFrame, company_id: str, import_id: str, s3_client: Any
) -> str:
    """
    Generate results CSV file with import status for each row.

    Uses the complete DataFrame with validation_error_message and is_duplicate
    columns to generate status without reloading the original file.

    Process:
    1. Use complete DataFrame with all rows
    2. Add 'import_status' column based on validation_error_message and is_duplicate
    3. Save to CSV with UTF-8 encoding
    4. Upload to S3 results folder

    Args:
        complete_df: Complete DataFrame with validation_error_message and is_duplicate columns
        company_id: Company identifier
        import_id: Import identifier
        s3_client: Boto3 S3 client

    Returns:
        S3 key for uploaded results file
    """
    logger.info(f'Gerando arquivo de resultados para {import_id}')

    # Create a copy for results
    df = complete_df.copy()

    # Add status column based on validation and duplicate flags
    def get_status(row):
        if row.get('validation_error_message', ''):
            return f'Falha - {row["validation_error_message"]}'
        elif row.get('is_duplicate', False):
            return 'Ignorado - Telefone duplicado'
        else:
            return 'Enviado para processamento'

    df['import_status'] = df.apply(get_status, axis=1)

    # Remove helper columns
    columns_to_drop = ['validation_error_message', 'is_duplicate']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # Save to temporary file
    results_file_path = f'/tmp/{import_id}-results.csv'
    df.to_csv(results_file_path, index=False, encoding='utf-8-sig')

    # Upload to S3
    date_prefix = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    results_key = f'results/{settings.stage}/{company_id}/{date_prefix}/{import_id}-results.csv'

    s3_client.upload_file(
        results_file_path,
        UPLOAD_BUCKET,
        results_key,
        ExtraArgs={'ContentType': 'text/csv; charset=utf-8'},
    )

    logger.info(f'Arquivo de resultados salvo: {results_key}')

    # Cleanup temp file
    if os.path.exists(results_file_path):
        os.remove(results_file_path)

    return results_key


def get_import_status(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Get import status with automatic timeout detection for polling.

    This endpoint is polled by the frontend to track import progress and
    requires Cognito authentication.
    It automatically detects stuck imports and marks them as timed out.

    Process:
    1. Extract importID from query parameters
    2. Query DynamoDB for import record
    3. Check for timeout if status is 'processing'
    4. Generate pre-signed URL for results file if completed
    5. Return status with all metrics

    Args:
        event: API Gateway event with queryStringParameters
        context: Lambda context

    Expected Query Parameters:
        importID: Import identifier (required)

    Response (200 OK):
        {
            "importID": "import-uuid",
            "status": "completed|processing|pending|failed|failed - timeout",
            "totalRows": 1000,
            "processedCount": 1000,
            "queuedCount": 950,
            "leadsCreatedCount": 950,
            "failedCount": 30,
            "duplicateCount": 20,
            "createdAt": "2026-01-27T10:00:00Z",
            "completedAt": "2026-01-27T10:15:00Z",
            "errorMessage": "...",
            "resultsFileUrl": "https://..."
        }

    Error Responses:
        400 - Missing importID parameter
        404 - Import not found
        500 - Internal server error
    """
    try:
        logger.info('Consultando status de importação')

        # Handle OPTIONS preflight
        if event.get('httpMethod') == 'OPTIONS':
            return response(status_code=200, message='', headers=CORS_HEADERS)

        # Extract authenticated user email from Cognito authorizer
        try:
            authorizer = event.get('requestContext', {}).get('authorizer', {})
            claims = authorizer.get('claims', {})
            authenticated_email = claims.get('email', '').strip().lower()

            if not authenticated_email:
                logger.error('Missing email in Cognito token')
                return response(
                    status_code=401,
                    message={
                        'error': 'Authentication error: email claim missing in token'
                    },
                    headers=CORS_HEADERS,
                )

            logger.info(f'Request authenticated for user: {authenticated_email}')
        except Exception as e:
            logger.error(f'Failed to extract authenticated user: {str(e)}')
            return response(
                status_code=401,
                message={
                    'error': 'Authentication error: unable to verify user identity'
                },
                headers=CORS_HEADERS,
            )

        # Extract importID from query parameters
        query_params = event.get('queryStringParameters') or {}
        import_id = query_params.get('importID', '').strip()

        if not import_id:
            return response(
                status_code=400,
                message={'error': 'Parâmetro importID é obrigatório'},
                headers=CORS_HEADERS,
            )

        # Query DynamoDB
        import_db = DatabaseHandler(table_name=settings.import_status_table_name)
        import_record_raw = import_db.get_item(key={'importID': import_id})

        if not import_record_raw:
            return response(
                status_code=404,
                message={'error': 'Importação não encontrada'},
                headers=CORS_HEADERS,
            )

        # Deserialize DynamoDB item
        import_record = import_db._deserialize_item(import_record_raw)

        # Check for timeout if still processing
        current_status = import_record.get('status')
        if current_status == 'processing':
            updated_at_str = import_record.get('updatedAt')
            if updated_at_str:
                try:
                    updated_at = datetime.fromisoformat(
                        updated_at_str.replace('Z', '+00:00')
                    )
                    age = datetime.now(timezone.utc) - updated_at

                    if age > timedelta(minutes=TIMEOUT_MINUTES):
                        # Mark as timeout
                        logger.warning(f'Importação excedeu timeout: {import_id}')

                        import_db.update_item(
                            key={'importID': import_id},
                            updates={
                                'status': 'failed - timeout',
                                'errorMessage': 'Processamento excedeu o limite de tempo',
                                'updatedAt': datetime.now(timezone.utc).isoformat(),
                            },
                            primary_key='importID',
                        )

                        # Update local record
                        import_record['status'] = 'failed - timeout'
                        import_record[
                            'errorMessage'
                        ] = 'Processamento excedeu o limite de tempo'

                except Exception as e:
                    logger.warning(f'Erro ao verificar timeout: {str(e)}')

        # Query actual lead count using importID-createdAt-index GSI
        leads_created_count = 0
        try:
            leads_db = DatabaseHandler(table_name=settings.leads_table_name)
            query_response = leads_db.client.query(
                TableName=settings.leads_table_name,
                IndexName='importID-createdAt-index',
                KeyConditionExpression='importID = :iid',
                ExpressionAttributeValues={':iid': {'S': import_id}},
            )
            # Deserialize items from DynamoDB format
            leads_results_raw = query_response.get('Items', [])
            leads_results = (
                [leads_db._deserialize_item(item) for item in leads_results_raw]
                if leads_results_raw
                else []
            )
            leads_created_count = len(leads_results) if leads_results else 0
            logger.info(
                f'Leads criados para importação {import_id}: {leads_created_count}'
            )
        except Exception as e:
            logger.warning(f'Erro ao consultar leads criados: {str(e)}')

        # Build response
        response_data = {
            'importID': import_id,
            'status': import_record.get('status'),
            'totalRows': int(import_record.get('totalRows', 0)),
            'processedCount': int(import_record.get('processedCount', 0)),
            'queuedCount': int(import_record.get('queuedCount', 0)),
            'leadsCreatedCount': leads_created_count,
            'failedCount': int(import_record.get('failedCount', 0)),
            'duplicateCount': int(import_record.get('duplicateCount', 0)),
            'createdAt': import_record.get('createdAt'),
        }

        # Add optional fields
        if 'errorMessage' in import_record:
            response_data['errorMessage'] = import_record['errorMessage']

        if 'completedAt' in import_record:
            response_data['completedAt'] = import_record['completedAt']

        # Generate pre-signed URL for results file if completed
        if (
            import_record.get('status') == 'completed'
            and 'resultsFileKey' in import_record
        ):
            try:
                s3_client = boto3.client('s3', region_name=settings.region)
                results_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': UPLOAD_BUCKET,
                        'Key': import_record['resultsFileKey'],
                    },
                    ExpiresIn=604800,  # 7 days (max allowed)
                )
                response_data['resultsFileUrl'] = results_url
            except Exception as e:
                logger.warning(f'Erro ao gerar URL de resultados: {str(e)}')

        logger.info(f'Status retornado: {import_record.get("status")}')

        return response(status_code=200, message=response_data, headers=CORS_HEADERS)

    except Exception as e:
        logger.error(f'Erro ao consultar status: {str(e)}', exc_info=True)
        return response(
            status_code=500,
            message={'error': 'Erro ao consultar status da importação'},
            headers=CORS_HEADERS,
        )
