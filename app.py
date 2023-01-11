import argparse
from http.client import CONTINUE
from logging import info, warning, error
import json
import logging
import os
from os import listdir, remove
import sys
from json import dump as jsondf
from json import dumps as jsond
from json import load as jsonl
from typing import Dict, List, Union
from uuid import uuid4

import uvicorn
from fastapi import Body, FastAPI, Path, Query, Response, BackgroundTasks
from fastapi.responses import HTMLResponse, PlainTextResponse
from models import AnySectionType, GenerationResponseModel, IntroSectionModel, LanguageEnum, PodcastModel, PreviewSectionModel, SectionTypes, SpeakerEnum

from podcast.audioController import AudioController
from podcast.contentManager import ContentManager
from podcast.podcast import Podcast
from podcast.uploaders.blobUploader import BlobStorageUploader
from podcast.weatherAPI import OpenWeatherAPI

#region Definitions

description = """
Raona Podcast Generator API can build automatic podcasts.
"""

tags_metadata = [
    {
        "name": "Webhooks",
        "description": "An auxiliary endpoint to use as Webhook for distributing videos.",
    },
    {
        "name": "Generation",
        "description": "Endpoints to use for generating videos automatically.",
    }
]

app = FastAPI(
    title="Raona Podcast Generator",
    description=description,
    version="1.0.0",
    openapi_tags=tags_metadata
    # terms_of_service="http://example.com/terms/"
)

CONFIG_FILE = "config_vanguardia.json"

#endregion

#region Helper functions

def generate_schema() -> None:
    """Dumps the App OpenAPI specification into a JSON file.
    """
    with open("openapi.json", 'w+') as afp:
        jsondf(app.openapi(), afp)

async def clean_up(directories: List[str]) -> None:
    for directory in directories:
        for file in listdir(directory):
            remove(f"{directory}/{file}")

async def bg_generate(podcast_config: PodcastModel, podcast: Podcast, filename: str, id_: str):
    # Podcast generation
    export_path, source_urls = podcast.__generate__(filename=filename,
                                                    format="mp3", 
                                                    clean_up=True, 
                                                    audio_config=podcast_config.audio.dict())
    
    # Podcast publication
    filename = podcast_config.storage.filename
    file_url = podcast.__upload__(id_=id_,
                                    path_to_mp3=export_path, 
                                    service="azblobstorage", 
                                    clean_up=True,
                                    extra_params=podcast_config.storage.dict())
    await clean_up(["placeholder", "exports"])

#endregion

#region Logging Handlers

log_formatter = logging.Formatter("%(levelname)s | %(asctime)s | [%(threadName)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
root_logger = logging.getLogger()
root_logger.propagate = False
root_logger.setLevel(logging.INFO)
if root_logger.handlers:
    console = root_logger.handlers[0]  # we assume the first handler is the one we want to configure
else:
    console = logging.StreamHandler()
    root_logger.addHandler(console)
console.setFormatter(log_formatter)
console.setLevel(logging.INFO)

#endregion


@app.post("/api/generate", tags=["Generation"], response_model=GenerationResponseModel)
async def post_generate(
        background_tasks: BackgroundTasks,
        podcast_config: PodcastModel=Body(...),) -> GenerationResponseModel:
    location = None
    try:
        location = [section.location for section in podcast_config.content if isinstance(section, IntroSectionModel)][0]
    except:
        warning("No location found on section Intro")
    
    try:
        content_config = [section.dict() for section in podcast_config.content]
        podcast = Podcast(
            podcast_config=f"config/{CONFIG_FILE}",
            content_config=content_config,
            location=location,
            org=podcast_config.org,
            name=podcast_config.name,
            speakers=podcast_config.speakers.dict(),
            language=podcast_config.language
        )
        
        id_ = str(uuid4())
        
        filename = podcast.generate_filename(id_=id_)
        file_url = BlobStorageUploader.precompute_url(name=podcast.name, 
                                                      id_=id_,
                                                      format="mp3",
                                                      az_credentials=podcast_config.storage.dict())
        background_tasks.add_task(bg_generate, podcast_config=podcast_config, podcast=podcast, filename=filename, id_=id_)
        
        result = {
            "file_url": file_url
        }
        return GenerationResponseModel(**result)
    except Exception as e:
        error(str(e))


@app.post("/api/previewSection", tags=["Preview"], response_class=PlainTextResponse)
async def post_preview_section(
        lang: LanguageEnum=Query(LanguageEnum.es),
        body: Union[AnySectionType, PreviewSectionModel] = Body(...)):
    if isinstance(body, PreviewSectionModel):
        preview_content_config = [body.section.dict()]
        if body.audio:
            audio_config = body.audio.dict()
        else:
            audio_config = {}
    else:
        preview_content_config = [body.dict()]
        audio_config = {}
    podcast = Podcast(
        podcast_config=f"config/{CONFIG_FILE}",
        content_config=preview_content_config,
        language=lang
    )
    export_path, _ = podcast.__generate__(format="mp3", 
                                          preview=True, 
                                          clean_up=True, 
                                          audio_config=audio_config)
    
    b64response = AudioController.encode(export_path)
    os.remove(export_path)
    return PlainTextResponse(str(b64response)[2:-1])
        

@app.post("/api/previewVoice", tags=["Preview"], response_class=PlainTextResponse)
async def post_voice_generator(
        lang: LanguageEnum=Query(LanguageEnum.es),
        voice: SpeakerEnum=Query(SpeakerEnum.male),
        text: str=Query(...)):
    export_path = AudioController.synthesize(text, 
                                    format="mp3", 
                                    voice=voice, 
                                    language=lang.value, 
                                    config_file=CONFIG_FILE)

    b64response = AudioController.encode(export_path)
    os.remove(export_path)
    return PlainTextResponse(str(b64response)[2:-1])


@app.post("/api/news", tags=["Utilities"])
async def post_retrieve_news(
        podcast_config: PodcastModel=Body(...)) -> Dict:
    location = None
    try:
        location = [section.location for section in podcast_config.content if isinstance(section, IntroSectionModel)][0]
    except:
        warning("No location found on section Intro")
        
    owa = OpenWeatherAPI()
    lat, lon = owa.city_name_to_lat_lon(location)
    metadata = {
        "geo": {
            "lat": lat,
            "lon": lon
        }
    }
    content = [section.dict() for section in podcast_config.content]
    _, urls = ContentManager.__ingest__(content_config=content, 
                                        metadata=metadata, 
                                        language=podcast_config.language)
    return urls


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--nodebug', action='store_false',
                        help='Whether the program needs to run an uvicorn debug server.')
    parser.add_argument('--schema', action='store_true',
                    help='Whether the program has to generate an openapi.json schema file.')

    args = parser.parse_args()

    if args.schema:
        generate_schema()
    if args.nodebug:
        uvicorn.run(app, host="0.0.0.0", port=8000)

