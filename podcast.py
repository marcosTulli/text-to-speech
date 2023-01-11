import json
import locale
import os
from datetime import datetime
from logging import info
from pathlib import Path
from random import choice
from typing import Dict, List, Tuple, Union
from uuid import uuid4

from tqdm import tqdm

from podcast.audioController import AudioController
from podcast.contentManager import ContentManager
from podcast.section import (Gimmick, Intro, NewsBody, NewsTitle, Outro,
                             Section, Transition)
from podcast.uploaders.blobUploader import BlobStorageUploader
from podcast.uploaders.buzzsproutUploader import BuzzsproutUploader
from podcast.uploaders.fileshareUploader import FileShareUploader
from podcast.weatherAPI import OpenWeatherAPI

owa = OpenWeatherAPI()

class Podcast(object):
    """Object that represents and generates a Podcast.

        Attributes:
            config (Dict): Podcast soundFx configuration.
            name (str): Podcast name.
            org (str): Podcast Organization.
            speakers (Dict): Podcast speaker data (names).
            location (str): Podcast city name.
            language (str): Podcast language.
            lat (float): Podcast city's latitude coordinate.
            lon (float): Podcast city's longitude coordinate.
            content_config (Dict): Podcast's content configuration.
    """

    def __init__(self, 
                podcast_config: str,
                content_config: Dict,
                location: str = None,
                org: str = "",
                name: str = "",
                speakers: Dict={"male": "Alberto", "female": "Mónica"},
                language: str="es"
                ) -> None:
        """Instantiates a Podcast

        Args:
            podcast_config (str): Path to the JSON of the Podcast configuration.
            content_config (Dict): Podcast's content configuration.
            location (str, optional): Podcast city name.. Defaults to "Barcelona".
            org (str, optional): Podcast Organization.. Defaults to "Garrigues".
            name (str, optional): Podcast name.. Defaults to "PodcastPlusUltra".
            speakers (Dict, optional): Podcast speaker data (names). Defaults to {"male": "Alberto", "female": "Mónica"}.
            language (str, optional): Podcast language. Defaults to "es".
        """
        if language == "es": locale.setlocale(locale.LC_ALL, "es_ES.UTF8")
        elif language == "ca": locale.setlocale(locale.LC_ALL, "ca_ES.UTF8")
        elif language == "es": locale.setlocale(locale.LC_ALL, "en_EN.UTF8")

        with open(podcast_config, "r", encoding="utf8") as f:
            self.config = json.load(f)
        self.name = name
        self.org = org
        self.speakers = speakers
        self.location = location
        self.language = language
        if self.location:
            self.lat, self.lon = lat, lon = owa.city_name_to_lat_lon(self.location)
        else:
            self.lat = self.lon = None
        self.content_config = content_config
        info(f"Processing podcast with name {self.name}")

    def ingest_content(self) -> List[Union[Transition, NewsTitle, NewsBody]]:
        """Converts dictionaries of content, to list of NewsTitle and NewsBody

        Args:
            content (List[Dict]): List of content for this podcast.
            max_news (int, optional): Maximum number of contents to take for the podcast. Defaults to 4.

        Returns:
            List[Union[section.NewsTitle, section.NewsBody]]: Sorted list of NewsTitle, NewsBody for every news that must be taken.
        """
        metadata = {
            "geo": {
            "lat": self.lat,
            "lon": self.lon
            }
        }
        content, urls = ContentManager.__ingest__(self.content_config, metadata, language=self.language)
        return content, urls

    def generate_intro(self, use_weather: bool=True) -> Intro:
        """Generates an introduction section based on templates.

        Args:
            use_weather (bool, optional): Defines if weather needs to be forecasted and used. Defaults to True.

        Returns:
            Intro: Object that represents the intro section of a podcast.
        """
        global owa 

        with open(f"templates/intros.json", "r", encoding="utf8") as f:
            intro_templates = json.load(f)[self.language]
        today = datetime.today()
        weekday, monthday, month = today.strftime('%A'), str(today.day), today.strftime('%B')
        if use_weather and self.location: # blanks are as follows: weekday, monthday, month, location, min_temp, max_temp, weather_descr
            try:
                min_temp, max_temp, weather_descr, weather_code = owa.get_temps_and_weather(self.lat, self.lon, self.language) # defaut: Barcelona
                weather_type = owa.weather_code_to_field(weather_code)
                if weather_type in intro_templates["weather_intros"].keys():
                    possible_templates = intro_templates["weather_intros"][weather_type]
                else: possible_templates = intro_templates["weather_intros"]["default"]
                intro = choice(possible_templates)
                intro["text"] = intro["text"].format(weekday,
                                                    monthday,
                                                    month,
                                                    self.location,
                                                    min_temp,
                                                    max_temp,
                                                    weather_descr,
                                                    self.org,
                                                    self.name)
            except: # blanks are as follows: weekday, monthday, month
                intro = choice(intro_templates["no_weather_intros"])
                today = datetime.today()
                weekday, monthday, month = today.strftime('%A'), str(today.day), today.strftime('%B')
                intro["text"] = intro["text"].format(weekday, monthday, month)
        else:
            possible_templates = intro_templates["no_weather_intros"]
            intro = choice(possible_templates)
            intro["text"] = intro["text"].format(weekday,
                                                    monthday,
                                                    month,
                                                    self.location,
                                                    "",
                                                    "",
                                                    "",
                                                    self.org,
                                                    self.name)
        intro = Intro(intro)
        if isinstance(self.content_config, dict):
            if "Intro" in self.content_config.keys():
                if "speaker" in self.content_config["Intro"].keys():
                    intro.voice = self.content_config["Intro"]["speaker"]
                if "text" in self.content_config["Intro"].keys():
                    intro.text = self.content_config["Intro"]["text"]
                self.content_config.pop('Intro', None)
        elif isinstance(self.content_config, list):
            i = 0
            for i, item in enumerate(self.content_config):
                if item["type"] == "Intro":
                    if "speaker" in item.keys():
                        intro.voice = item["speaker"]
                    if "text" in item.keys():
                        intro.text = item["text"]  
                    break
            self.content_config = self.content_config[:i] + self.content_config[i+1:]
        else:
            raise ValueError(f"Content config cannot be {type(self.content_config)}")
        info(f"Generated introduction: '{intro.text}'")
        return intro

    def generate_outro(self) -> Outro:
        """Generates an outro section based on templates.

        Returns:
            Outro: Object that represents the outro section of a podcast.
        """
        with open(f"templates/outros.json", "r", encoding="utf8") as f:
            outro_templates = json.load(f)[self.language]
        outro = choice(outro_templates)
        outro = Outro(outro)

        if isinstance(self.content_config, dict):
            if "Outro" in self.content_config.keys():
                if "speaker" in self.content_config["Outro"].keys():
                    outro.voice = self.content_config["Outro"]["speaker"]
                if "text" in self.content_config["Outro"].keys():
                    outro.text = self.content_config["Outro"]["text"]
                self.content_config.pop('Outro', None)
        elif isinstance(self.content_config, list):
            i = 0
            for i, item in enumerate(self.content_config):
                if item["type"] == "Outro":
                    if "speaker" in item.keys():
                        outro.voice = item["speaker"]
                    if "text" in item.keys():
                        outro.text = item["text"]  
                    break
            self.content_config = self.content_config[:i] + self.content_config[i+1:]
        else:
            raise ValueError(f"Content config cannot be {type(self.content_config)}")

        return outro

    def generate_gimmick(self) -> Gimmick:
        """Generates a random gimmick section for the podcast.

        Returns:
            Gimmick: The randomly selected gimmick.
        """
        with open(f"templates/gimmicks.json", "r", encoding="utf8") as f:
            gimmicks = json.load(f)[self.language]
        gimmick = choice(gimmicks)
        gimmick = Gimmick(gimmick)
        return gimmick

    def generate_transitions(self) -> Tuple[Transition, Transition, Transition, Transition]:
        """Finds transitions for different sections of the podcast

        Returns:
            Tuple[Transition, Transition, Transition, Transition]:
                - Transition from intro to content
                - Transition to the actual content
                - Transition to the last piece of content
                - Transition to the outro
        """
        with open(f"templates/transitions.json", "r", encoding="utf8") as f:
            transitions = json.load(f)[self.language]
        intro_to_content = choice(transitions["intro_to_content"])
        content_intro_to_content_info = choice(transitions["content_intro_to_content_info"])
        to_last_content = choice(transitions["to_last_content"])
        to_outro = choice(transitions["to_outro"])

        intro_to_content["text"] = intro_to_content["text"]
        content_intro_to_content_info["text"] = content_intro_to_content_info["text"]
        to_last_content["text"] = to_last_content["text"]
        to_outro["text"] = to_outro["text"]
        return Transition(intro_to_content), Transition(content_intro_to_content_info), Transition(to_last_content), Transition(to_outro)

    def process_podcast(self, 
                        synthesize: bool=True, 
                        preview: bool=False) -> Tuple[List[Section], List[object]]:
        """Processes intro, content and outro to generate audio files via Azure TTS api.

        Args:
            synthesize (bool, optional): Defines if AzureTTS will be used or last audios will be reused. Defaults to True.
            preview (bool, optional): Defines wether it is using preview Model Defaults to False.
            
        Returns: Tuple[List[Section], List[object]]
            List[Section]: List of Podcast's sections.
            List[object]: Object with content urls for each source.
        """
        
        if not preview:
            intro = self.generate_intro()
            outro = self.generate_outro()
            intro_to_content, content_intro_to_content_info, to_last_content, to_outro = self.generate_transitions()

            has_gimmick = any([content["type"] == "gimmick" for content in self.content_config])
            if has_gimmick:
                gimmick = self.generate_gimmick()

        content, source_urls = self.ingest_content()


        if not preview:
            try:
                # For the transition to last content, if last content itself has a transition
                # we respect it and put it after.
                if not isinstance(content[-3], Transition):
                    first_half = content[:-2]
                    first_half[-1].has_transition = False
                    second_half = content[-2:]
                    second_half[0].is_news = False
                    second_half[1].is_news = False
                # Otherwise, we take the last title-body of the content
                # and prepare it to be after the transition to last content.
                else:
                    first_half = content[:-3]
                    first_half[-1].has_transition = False
                    second_half = content[-3:]
                    second_half[0].is_news = False
                    second_half[1].is_news = False   
                    second_half[2].is_news = False         
            except:
                raise IndexError("Podcast must have more than 1 content.")
            # Configure speaker for transitions
            intro_to_content.voice = "male" if first_half[0].voice == "female" else "female"
            content_intro_to_content_info.voice = first_half[0].voice
            to_last_content.voice = "male" if second_half[0].voice == "female" else "female"
            to_outro.voice = to_last_content.voice



            if first_half[0].voice == intro.voice:
                intro.has_transition = True
                introduction = [intro]
            else:
                intro_to_content.text = intro_to_content.text.format(self.speakers[first_half[0].voice])
                content_intro_to_content_info.text = content_intro_to_content_info.text.format(self.speakers[intro.voice])
                
                introduction = [intro, intro_to_content, content_intro_to_content_info]

            if second_half[-1].voice == outro.voice:
                if has_gimmick:
                    outroduction = [gimmick, outro]
                else: 
                    outroduction = [outro]
            else:
                to_outro.text = to_outro.text.format(self.speakers[second_half[-1].voice])
                if has_gimmick:
                    outroduction = [to_outro, gimmick, outro]
                else:
                    outroduction = [to_outro, outro]
            podcast = introduction + first_half + [to_last_content] + second_half + outroduction
        
        else:
            podcast = content

        info(f"This podcast contains {len( ''.join([ s.text for s in podcast ]))} chars.")

        for i, section in enumerate(podcast):
            info(f"{i}: {section.text}")

        info("Processing and synthesizing podcast.")
        for i, section in tqdm(list(enumerate(podcast))):
            if not section.audio_path:
                if synthesize:
                    file_path = AudioController.synthesize(text=section.text, format="wav", voice=section.voice, language=self.language)
                else:
                    file_path = sorted(Path("placeholder").iterdir(), key=os.path.getmtime)[i]._str
                section.audio_path = file_path

        for section in podcast: info("Generated: "+section.audio_path)

        return podcast, source_urls

    def generate_filename(self, id_: str) -> str:
        date = datetime.today().strftime("%Y-%m-%d_%H%M%S")
        filename = f"{self.org}-{date}-{id_}"
        return filename

    def __generate__(self,
                    filename: str,
                    format: str="mp3", 
                    clean_up: bool=True, 
                    audio_config: Dict={}, 
                    preview: bool=False) -> Tuple[str, Dict]:
        """Generates a Podcast and stores it into disk.

        Args:
            format (str, optional): The audio format in which the podcast will be stored. Defaults to "mp3".
            clean_up (bool, optional): Defines if it has to clean all the placeholder audio files at the end. Defaults to True.
            audio_config (Dict, optional): Maps the effect IDs to url sounds to apply. Defaults to {}.
            preview (bool, optional): Defines wether it is using preview Model Defaults to False.
        
        Returns:
            Tuple[str, Dict]:
                - filepath of the exported podcast.
                - dictionary of source urls from where the content was taken.
        """
        filepath = f"exports/{filename}"
        podcast, source_urls = self.process_podcast(synthesize=True, preview=preview)
        generated_podcast = AudioController.merge_audio(podcast, self.config, audio_config, preview=preview)

        AudioController.export(generated_podcast, filepath, format=format)
        filepath += '.'+format

        if clean_up:
            for section in podcast:
                try: os.remove(section.audio_path)
                except: continue
            info("Deleted placeholder TTS files.")
        return filepath, source_urls

    def __upload__(self, 
                   id_: str,
                   path_to_mp3: str,
                   service: str, 
                   clean_up: bool=True,
                   extra_params: Dict={}) -> str:
        """Uploads export to service.

        Args:
            path_to_mp3 (str): Path to export.
            service (str): Podcast service to upload to.
            clean_up (bool, optional): Flag to remove the export after uploading. Defaults to True.

        Raises:
            NotImplementedError: If service is not compatible yet.

        Returns:
            - The url where the podcast was uploaded to.
        """
        info(f"Uploading podcast to {service}.")
        if service == "azfileshare":
            podcast_url = FileShareUploader.__upload__(path_to_mp3)

        elif service == "azblobstorage":
            podcast_url = BlobStorageUploader.__upload__(filepath=path_to_mp3, name=self.name, id_=id_, is_mp3=True, az_credentials=extra_params)

        elif service == "buzzsprout":
            date = datetime.today().strftime("%d/%m/%Y")
            title = f"Rapodcast - {date}"
            content = ""
            podcast_url = BuzzsproutUploader.__upload__(path_to_mp3, title, content)
        else:
            podcast_url = "NotImplementedError"
            raise NotImplementedError("Connector to service not done yet.")
        info(f"Podcast uploaded.")
        if clean_up:
            os.remove(path_to_mp3)
            info(f"Deleted internal export.")
        return podcast_url

