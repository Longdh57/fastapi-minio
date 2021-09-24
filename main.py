import uvicorn

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from io import BytesIO
from fastapi import File, UploadFile, Path
from starlette.responses import StreamingResponse

from minio_handler import MinioHandler


def get_application() -> FastAPI:
    application = FastAPI(
        title='FastAPI with Minio',
        description='Integrate FastAPI with Minio',
        openapi_url="/openapi.json",
        docs_url="/docs"
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return application


app = get_application()


@app.get('/', tags=[''])
def get():
    return 'Hello World'


class CustomException(Exception):
    http_code: int
    code: str
    message: str

    def __init__(self, http_code: int = None, code: str = None, message: str = None):
        self.http_code = http_code if http_code else 500
        self.code = code if code else str(self.http_code)
        self.message = message


class UploadFileResponse(BaseModel):
    bucket_name: str
    file_name: str
    url: str


@app.post("/upload/minio", response_model=UploadFileResponse)
async def upload_file_to_minio(file: UploadFile = File(...)):
    try:
        data = file.file.read()

        file_name = " ".join(file.filename.strip().split())

        data_file = MinioHandler().get_instance().put_object(
            file_name=file_name,
            file_data=BytesIO(data),
            content_type=file.content_type
        )
        return data_file
    except CustomException as e:
        raise e
    except Exception as e:
        if e.__class__.__name__ == 'MaxRetryError':
            raise CustomException(http_code=400, code='400', message='Can not connect to Minio')
        raise CustomException(code='999', message='Server Error')


@app.get("/download/minio/{filePath}")
def download_file_from_minio(
        *, filePath: str = Path(..., title="The relative path to the file", min_length=1, max_length=500)):
    try:
        minio_client = MinioHandler().get_instance()
        if not minio_client.check_file_name_exists(minio_client.bucket_name, filePath):
            raise CustomException(http_code=400, code='400',
                                  message='File not exists')

        file = minio_client.client.get_object(minio_client.bucket_name, filePath).read()
        return StreamingResponse(BytesIO(file))
    except CustomException as e:
        raise e
    except Exception as e:
        if e.__class__.__name__ == 'MaxRetryError':
            raise CustomException(http_code=400, code='400', message='Can not connect to Minio')
        raise CustomException(code='999', message='Server Error')


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)
