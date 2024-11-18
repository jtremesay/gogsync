#!/usr/bin/env python3
import asyncio
import json
import logging
from argparse import ArgumentParser, Namespace
from collections.abc import AsyncGenerator, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Iterable, Optional
from xml.etree import ElementTree

from aiofile import async_open
from aiohttp import ClientSession

logger = logging.getLogger(__name__)
from dataclasses import dataclass, field


class Platform(StrEnum):
    WINDOWS = "windows"
    MAC = "mac"
    LINUX = "linux"


class Language(StrEnum):
    ENGLISH = "English"
    GERMAN = "Deutsch"
    SIMPLIFIED_CHINESE = "中文(简体)"
    SPANISH_AL = "Español (AL)"
    ARABIC = "العربية"
    TURKISH = "Türkçe"
    JAPANESE = "日本語"
    PORTUGUESE = "português"
    SPANISH = "español"
    POLISH = "polski"
    FRENCH = "français"
    RUSSIAN = "русский"
    BRAZILIAN_PORTUGUESE = "Português do Brasil"
    ITALIAN = "italiano"


@dataclass
class ContentFile:
    name: str
    url: str
    version: str
    platform: Platform
    language: Language
    size: int

    def serialize(self):
        return {
            "name": self.name,
            "url": self.url,
            "version": self.version,
            "platform": self.platform.value,
            "language": self.language.value,
            "size": self.size,
        }

    @classmethod
    def unserialize(cls, data):
        return cls(
            name=data["name"],
            url=data["url"],
            version=data["version"],
            platform=Platform(data["platform"]),
            language=Language(data["language"]),
            size=data["size"],
        )


@dataclass
class DLC:
    title: str
    cd_key: str
    files: list[ContentFile] = field(default_factory=list)

    def serialize(self):
        return {
            "title": self.title,
            "cd_key": self.cd_key,
            "files": [f.serialize() for f in self.files],
        }

    @classmethod
    def unserialize(cls, data):
        return cls(
            title=data["title"],
            cd_key=data["cd_key"],
            files=[ContentFile.unserialize(f) for f in data["files"]],
        )


@dataclass
class Game:
    id: str
    title: str
    cd_key: str
    files: list[ContentFile] = field(default_factory=list)
    dlcs: list[DLC] = field(default_factory=list)

    def serialize(self):
        return {
            "id": self.id,
            "title": self.title,
            "cd_key": self.cd_key,
            "files": [f.serialize() for f in self.files],
            "dlcs": [d.serialize() for d in self.dlcs],
        }

    @classmethod
    def unserialize(cls, data):
        return cls(
            id=data["id"],
            title=data["title"],
            cd_key=data["cd_key"],
            files=[ContentFile.unserialize(f) for f in data["files"]],
            dlcs=[DLC.unserialize(d) for d in data["dlcs"]],
        )


async def load_credentials():
    async with async_open("credentials.json") as f:
        data = await f.read()
    credentials = json.loads(data)

    return credentials


async def load_games():
    async with async_open("games.json") as f:
        data = await f.read()
    games_data = json.loads(data)
    games = [Game.unserialize(game_data) for game_data in games_data]

    return games


async def aget_file_size(url: str, session: ClientSession) -> int:
    url = "https://www.gog.com" + url
    logger.info("fetching file size for %s", url)
    async with session.get(url) as response:
        file_size = int(response.headers["Content-Length"])
    logger.debug("file size for %s is %d", url, file_size)
    return file_size


async def aupdate_file_metadata(file: ContentFile, session: ClientSession):
    logger.info("fetching metada for file %s", file.url)

    url = "https://www.gog.com" + file.url

    # The https://www.gog.com/downloads/<game>/<file_id> url redirect to
    # an https://gog-cdn-fastly.gog.com/XXX/<file_name> url
    async with session.get(url) as response:
        url = response.url

        # Get the real file name and size from url and headers
        file.name = url.path.split("/")[-1]
        file.size = int(response.headers["Content-Length"])
        logger.info("url %s file %s is %d bytes", url, file.name, file.size)


async def afetch_game(
    id: str, session: ClientSession, fetch_size: bool = False
) -> Optional[Game]:
    logger.debug("fetching data for game %s", id)
    data = await (
        await session.get(f"https://www.gog.com/account/gameDetails/{id}.json")
    ).json()
    if not data:
        logger.warning("no data found for game %s", id)
        return None

    logger.info("found game %s for id %s", data["title"], id)

    dlcs = []
    for dlc_data in data["dlcs"]:
        files = []
        for language, language_data in dlc_data["downloads"]:
            language = Language(language)
            for platform, platform_data in language_data.items():
                platform = Platform(platform)
                for file in platform_data:
                    files.append(
                        ContentFile(
                            name=file["name"],
                            url=file["manualUrl"],
                            version=file["version"],
                            platform=platform,
                            language=language,
                            size=(
                                await aget_file_size(file["manualUrl"], session)
                                if fetch_size
                                else 0
                            ),
                        )
                    )

        dlc = DLC(
            title=dlc_data["title"],
            cd_key=dlc_data["cdKey"],
            files=files,
        )
        dlcs.append(dlc)

    files = []
    for language, language_data in data["downloads"]:
        language = Language(language)
        for platform, platform_data in language_data.items():
            platform = Platform(platform)
            for file in platform_data:
                files.append(
                    ContentFile(
                        name=file["name"],
                        url=file["manualUrl"],
                        version=file["version"],
                        platform=platform,
                        language=language,
                        size=(
                            await aget_file_size(file["manualUrl"], session)
                            if fetch_size
                            else 0
                        ),
                    )
                )

    game = Game(
        id=id,
        title=data["title"],
        cd_key=data["cdKey"],
        files=files,
        dlcs=dlcs,
    )

    return game


async def aget_games(
    ids: Iterable[str], session: ClientSession
) -> AsyncGenerator[Game, None]:
    games = [
        g for g in await asyncio.gather(*(afetch_game(id, session) for id in ids)) if g
    ]

    # build the list of all files
    files = []
    for game in games:
        files.extend(game.files)
        for dlc in game.dlcs:
            files.extend(dlc.files)

    # files = [
    #     f
    #     for f in files
    #     if f.platform in (Platform.LINUX,)  # Platform.WINDOWS, Platform.MAC, )
    #     and f.language in (Language.ENGLISH, Language.FRENCH)
    # ]

    # fetch the size of all files
    logger.info("fetching metada for %d files…", len(files))
    await asyncio.gather(*(aupdate_file_metadata(f, session) for f in files))

    return games


async def aupdate(args: Namespace, session: ClientSession):
    logger.info("fetching licences…")
    r = await session.get("https://menu.gog.com/v1/account/licences")
    licenses = await r.json()
    logger.info("found %d licences", len(licenses))

    games = await aget_games(licenses, session)
    games_data = json.dumps([g.serialize() for g in games], indent=2)
    async with async_open("games.json", "w") as f:
        await f.write(games_data)


async def adownload_file(file: ContentFile, session: ClientSession):
    dir_name = file.url.split("/")[-2]
    dir_path = Path("games") / dir_name
    dir_path.mkdir(parents=True, exist_ok=True)

    file_path = dir_path / file.name

    if not file_path.exists() or file.size != file_path.stat().st_size:
        logger.info("downloading %s from %s", file_path, file.url)
        async with session.get("https://www.gog.com" + file.url) as response:
            async with async_open(file_path, "wb") as f:
                size = 0
                chunk_size = 4 * 1024 * 1024
                async for chunk in response.content.iter_chunked(chunk_size):
                    chunk_size = len(chunk)
                    size += chunk_size
                    logger.debug(
                        "writing %d bytes (%d/%d) to %s",
                        chunk_size,
                        size,
                        file.size,
                        file_path,
                    )
                    await f.write(chunk)
        logger.info("downloaded %s", file_path)
    else:
        logger.info("skipping %s", file_path)


async def adownload(args: Namespace, session: ClientSession):
    games = await load_games()

    # build the list of all files
    files = []
    for game in games:
        files.extend(game.files)
        for dlc in game.dlcs:
            files.extend(dlc.files)

    # Download all files
    logger.info("downloading %d files…", len(files))
    await asyncio.gather(*(adownload_file(f, session) for f in files))


async def aclean(args: Namespace, _: ClientSession):
    pass


async def asandbox(args: Namespace, session: ClientSession):
    pass


async def amain(args: Optional[Sequence[str]] = None):
    logging.basicConfig(level=logging.INFO)

    parser = ArgumentParser()

    sub_parsers = parser.add_subparsers(dest="command")

    update_parser = sub_parsers.add_parser("update", help="Update the game files list")
    update_parser.set_defaults(func=aupdate)

    download_parser = sub_parsers.add_parser("download", help="Download games files")
    download_parser.set_defaults(func=adownload)

    clean_parser = sub_parsers.add_parser(
        "clean", help="Clean unreferenced and corrupted game files"
    )
    clean_parser.set_defaults(func=aclean)

    sandbox_parser = sub_parsers.add_parser("sandbox")
    sandbox_parser.set_defaults(func=asandbox)

    args = parser.parse_args()

    if args.command is None:
        parser.print_usage()
        return

    credentials = await load_credentials()
    async with ClientSession(cookies=credentials) as session:
        await args.func(args, session)


if __name__ == "__main__":
    asyncio.run(amain())
