#!/usr/bin/python

# Import statements / Settings
import logging
import os
import os.path
import stat
import subprocess
import MySQLdb
import urlparse
import urllib
import re
import tempfile
import glob
import sys
import multiprocessing as threading
from random import randint
from time import sleep

import requests
from pymediainfo import MediaInfo
# Utility Functions #

mysql = {
    "hostname": os.environ["MYSQL_HOSTNAME"],
    "username": os.environ["MYSQL_USERNAME"],
    "password": os.environ["MYSQL_PASSWORD"],
    "database": os.environ["MYSQL_DATABASE"],
}
logging.basicConfig(format='%(asctime)s [%(levelname)-8s]: %(message)s',
                    datefmt='%H:%M:%S%p', level=logging.DEBUG)


# Function run mySQL queries
def execute_mySQL(query, entryID=-1, select=False):
    """
    Run MySQL queries against the mediaPanel database
    """
    if entryID != -1:
        logging.info("Entry {0}: {1}".format(entryID, query))
    else:
        logging.info("{0}".format(query))
    con = MySQLdb.connect(
            host=mysql["hostname"],
            user=mysql["username"],
            passwd=mysql["password"],
            db=mysql["database"])
    with con.cursor() as cur:
        cur.execute(query)
        if select:
            fields = map(lambda x: x[0], cur.description)
            result = [dict(zip(fields, row)) for row in cur.fetchall()]
            return(result)


def getUrlFileName(url):
    """Determine the full name of a file from a given URL."""
    url_object = urlparse.urlsplit(url)
    return urllib.unquote(os.path.basename(url_object.path))


def getRequestsFileName(request):
    """Get the filename from a Requests context"""
    content_disposition = request.headers.get("Content-Disposition")
    if content_disposition is not None:
        attributes = (x.strip() for x in content_disposition.split(";")
                      if x.startswith("filename="))
        for attr in attributes:
            _, filename = attr.split("=")
            return filename.strip('"')
    return None


def downloadFile(url, filename=None, filepath=None, overwrite=True,
                 prefix=None):
    """Download a file from a URL to a specific path."""
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        # get filename if one is not passed in
        assert filename is not None and filepath is not None
        if "Content-Disposition" in r.headers.keys() and filename is None:
            basename = getRequestsFileName(r)
            if basename is not None:
                filename = os.path.join(filepath, basename)
        if filename is None:
            filename = os.path.join(filepath, getUrlFileName(url))

        if prefix is not None:
            filename = prefix + filename

        # iterate over the file and write to the filepath
        with open(filepath, "w") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return filename


# Gets the meta data of a file
def getMediaData(filename):
    """
    Get media data for a file: filePath, mediaType, rotation, imageType.

    Only filePath is guaranteed to not be a default, otherwise:

    - mediaType is "unsupported"
    - rotation is 0
    - imageType is "undefined"

    - mediaTypes will be a comma-delimited unsorted list of filetypes
    """
    media_info = MediaInfo.parse(filename)
    media = {
        "filePath": filename,
        "mediaType": "unsupported",
        "rotation": 0,
        "imageType": "undefined",
    }

    # Iterate over media to get "highest" priority content
    # Looking for: PDF, Video, Audio, Image
    media_types = set()
    for item in media_info.tracks:
        media_types.add(item.track_type)  # Video, Audio, Image
        media_types.add(item.format)  # PDF

    media["mediaTypes"] = ",".join(media_types)

    # Reassign as the priority gets higher; Video can have Audio tracks
    for x in ["Image", "Audio", "Video", "PDF"]:
        if x in media_types:
            media["mediaType"] = x

    # Determine rotation for images or videos, used for flattening server-side
    for item in media_info.tracks:
        if item.rotation is not None:
            media["rotation"] = item.rotation

    # If the media is an image, determine the imageType via Format
    for item in filter(lambda x: x.track_type == "Image", media_info.tracks):
        media["imageType"] = item.format

    return media


# Threading functions #
# Threading control function
# ::TODO:: remove when moving to multiprocessing pool
def threadInProgress(inProgress, lock, state):
    if state == 1:
        lock.acquire()
        inProgress.value += 1
        lock.release()
    elif state == 0:
        lock.acquire()
        inProgress.value -= 1
        lock.release()
    else:
        lock.acquire()
        temp = inProgress.value
        lock.release()
        return temp


# ::TODO:: refactor when moving bootFreeDiskAmount to be 64 bit integer bytes
# Device / Group Storage Protection
def convertSizeStringToBytes(sizeString):
    value = float(''.join(re.findall('[^A-Z]', sizeString)))
    letter = re.findall('[A-Z]', sizeString)[0]

    if letter == "G":
        return (value * 1024 * 1024 * 1024)
    elif letter == "M":
        return (value * 1024 * 1024)
    elif letter == "K":
        return (value * 1024)
    else:
        return value


def getFolderSize(folderPath):
    try:
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(folderPath):
            if "/uploaded" not in dirpath:
                for f in filenames:
                    filepath = os.path.join(dirpath, f)
                    total_size += os.path.getsize(filepath)
        return total_size
    except Exception as e:
        logging.warning("%r", e)
    return(0)


def get_fileSize(filePath):
    try:
        return(os.path.getsize(filePath))
    except Exception as e:
        logging.warning(e)
    return(0)


def checkStorageLimitation(entry, pathToFile, multiplier):
    """
    Determine free space for either a device which may be in multiple groups,
    or a group which may contain multiple devices.

    Expects an entry dict:
    - mode: ("device", "group") -- whether or not it's a device or group file
    - filePath: str -- location of the file itself
    - deviceID | groupID: str -- the ID of either the device or the group
    - clientID: str -- the ID of the client
    - id: str -- the ID of the processing request
    """
    # Check device and all groups for how much space is taken up
    if "device" in entry["mode"]:
        filePath = entry["filePath"]
        pathToClient = filePath[:filePath.find("/1/%s" % entry["deviceID"])]
        logging.debug(pathToClient)

        # Gets the devices SD Card Size
        maxSize = execute_mySQL((
            "SELECT bootTotalDiskSize FROM devices "
            "WHERE deviceID = '{0}'").format(entry["deviceID"]),
            entry["id"], True)[0]["bootTotalDiskSize"]

        # Gets the storage use of the device
        deviceFolder = os.path.join(pathToClient, "1", entry["deviceID"])
        deviceSize = getFolderSize(deviceFolder)

        # Gets the storage use of all groups the device may be in
        groupSize = 0
        groups_query = """SELECT device2groups.groupID FROM device2groups
        WHERE device2groups.deviceID = '{0}'""".format(entry["deviceID"])
        for group in execute_mySQL(groups_query, entry["id"], True):
            group_folder = os.path.join(pathToClient, str(group["groupID"]))
            groupSize += getFolderSize(group_folder)

    # Check all devices in group for free space
    elif "group" in entry["mode"]:
        devices_query = """
           SELECT devices.deviceID, bootFreeDiskAmount as freeDisk,
                  bootTotalDiskSize as totalDisk
             FROM devices
        LEFT JOIN device2groups USING (deviceID)
            WHERE device2groups.groupID = '{0}'
              AND devices.clientID = '{1}'
        """.format(entry["groupID"], entry["clientID"])
        # Gets all the devices in the group
        devicesInGroup = execute_mySQL(devices_query, entry["id"], True)

        # Finds the device with the lowest free space
        if len(devicesInGroup) > 0:
            lowest_value = -1
            lowest_device = ""
            lowest_max = ""
            for device in devicesInGroup:
                file_size = convertSizeStringToBytes(device["freeDisk"])
                if lowest_value == -1 or file_size < lowest_value:
                    lowest_value = file_size
                    lowest_device = device["deviceID"]
                    lowest_max = device["totalDisk"]

            maxSize = lowest_max

            # Gets the storage use of the device with least reported free space
            deviceSize = getFolderSize(os.path.join(
                    "/var/www/html/mediapanel/device_config",
                    entry["clientID"], "1", lowest_device))

            groupSize = getFolderSize(
                    entry["filePath"].replace("/uploaded/", ""))

        else:
            return True

    SDCard = convertSizeStringToBytes(maxSize)
    reserved = (5*1024*1024*1024)
    freeSpace = SDCard - deviceSize - groupSize - reserved  # 5GB reserved
    maxuse = SDCard-reserved
    logging.debug("SD  : {0}GB {1}MB {2}KB".format(
        SDCard/1024/1024/1024, SDCard/1024/1024, SDCard/1024))
    logging.debug("Save: {0}GB {1}MB {2}KB".format(
        reserved/1024/1024/1024, reserved/1024/1024, reserved/1024))
    logging.debug("dUse: {0}GB {1}MB {2}KB".format(
        deviceSize/1024/1024/1024, deviceSize/1024/1024, deviceSize/1024))
    logging.debug("gUse: {0}GB {1}MB {2}KB".format(
        groupSize/1024/1024/1024, groupSize/1024/1024, groupSize/1024))
    logging.debug("max : {0}GB {1}MB {2}KB".format(
        maxuse/1024/1024/1024, maxuse/1024/1024, maxuse/1024))
    logging.debug("free: {0}GB {1}MB {2}KB".format(
        freeSpace/1024/1024/1024, freeSpace/1024/1024, freeSpace/1024))

    needed = get_fileSize(pathToFile) * multiplier
    logging.debug("Need {0}KB of {1}KB".format(needed/1024, freeSpace/1024))

    if needed < freeSpace:
        return True
    return False


# URL Downloader
# Uses youtube-dl to download youtube videos
def downloadYoutubeURL(url, filePath):
    des = filePath+"%(extractor)s-%(title)s-%(format)s.%(ext)s"

    info = subprocess.Popen(["youtube-dl", "-f", "22/18/mp4",
                             "--restrict-filenames", url, "-o", des],
                            stdout=subprocess.PIPE).communicate()[0]
    logging.debug(info)
    info2 = info.lower()
    if info2.find("error") == -1 and (
            "destination" in info2 or "has already been downloaded" in info2):
        spot = info.find("youtube-")
        if spot > -1:
            info = info[spot:]
            end = info.find(".mp4")
            fileName = info[:end+4]
            return fileName
    return ""


# ::TODO:: use multiprocessing.Pool
# Runs a thread for each url that needs to be downloaded
def urlDownloader(threadLimit):
    logging.info("Started process to download videos")
    # Create threading variables
    inProgress = threading.Value('i', 0)
    inProgressLock = threading.RLock()
    threads = []

    # get the database entries
    query = "SELECT * FROM urlDownloader"
    for entry in execute_mySQL(query, select=True):
        # Wait for an open thread
        while True:
            if threadInProgress(inProgress, inProgressLock, 3) < threadLimit:
                break
            else:
                sleep(1)

        # Start a thread to process the entry
        threads.append(threading.Process(
         target=processURLEntry, args=(entry, inProgress, inProgressLock)))
        threads[len(threads)-1].start()

    # wait for each thread to complete and join it back in.
    if len(threads) < 1:
        logging.info("Nothing to Download")
    for thread in threads:
        thread.join()
    logging.info("Completed process to download videos")


# Process a url that needs to be downloaded
# ::TODO:: de-thread threadInProgress
def processURLEntry(entry, inProgress, lock):
    threadInProgress(inProgress, lock, 1)

    if "youtu" in entry["url"] or "vimeo" in entry["url"]:
        fileName = downloadYoutubeURL(entry["url"], entry["filePath"])
    else:
        fileName = downloadFile(entry["url"], filepath=entry["filePath"],
                                prefix="downloaded_")

    if len(fileName) > 1:
        execute_mySQL(
                "INSERT INTO mediaConvertQueue "
                "(clientID,deviceID,groupID,filePath,fileName,mode,"
                "app_digitalFrame,app_displayAD,app_alerts,app_jukebox) "
                "VALUES("
                "{0},'{1}',{2},'{3}','{4}','{5}',{6},{7},{8},{9})".format(
                    entry["clientID"], entry["deviceID"], entry["groupID"],
                    entry["filePath"].strip(), fileName, entry["mode"],
                    entry["app_digitalFrame"], entry["app_displayAD"],
                    entry["app_alerts"], entry["app_jukebox"]),
                entry["id"])

    execute_mySQL('DELETE FROM urlDownloader WHERE id={0}'.format(entry["id"]),
                  entry["id"])

    threadInProgress(inProgress, lock, 0)


# Media Formatter
# Runs a thread for each file that needs to be formatted
# ::TODO:: remove threadInProgress somehow
def mediaFormat(threadLimit, avoidMP4=False):
    logging.info("Started convert Queue")
    # Create threading variables
    inProgress = threading.Value('i', 0)
    inProgressLock = threading.RLock()
    threads = []

    # get the database entries
    query = ("SELECT * FROM mediaConvertQueue WHERE status is NULL AND "
             "convertTimestamp is NULL")
    if avoidMP4:
        query += " AND RIGHT(fileName, 4) <> '.mp4'"
    query += " ORDER BY uploadTimestamp"
    for entry in execute_mySQL(query, -1, True):
        # Wait for an open thread
        while True:
            if threadInProgress(inProgress, inProgressLock, 3) < threadLimit:
                break
            else:
                sleep(1)

        # Start a thread to process the entry
        threads.append(threading.Process(
         target=processFormatEntry, args=(entry, inProgress, inProgressLock)))
        threads[len(threads)-1].start()

    # wait for each thread to complete and join it back in.
    if len(threads) < 1:
        logging.info("Nothing to format")
    for thread in threads:
        thread.join()
    logging.info("Completed convert Queue")


# checks state of file
def checkFileUpdateStatus(mediaData, entryID):
    status = "started_conversion"
    if (not os.path.exists(mediaData["filePath"])):
        status = "missing_File"
    elif mediaData["mediaType"] == "unsupported":
        status = "Invalid_mediaTypes={0}".format(mediaData["mediaTypes"])
    else:
        try:
            os.chmod(mediaData["filePath"],
                     stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            with open(mediaData["filePath"]):
                logging.debug("Entry {0}: Valid_File".format(entryID))
        except Exception as e:
            logging.warning("Entry %s: Error chmod: %r", entryID, e)
            status = "file_not_ready"
    return status


def updateMediaItemStatus(entryID, status, timeStamp=False):
    if timeStamp:
        query = """
        UPDATE mediaConvertQueue
           SET status='{}', convertTimestamp=NOW()
         WHERE id={}""".format(status, entryID)
    else:
        query = """
        UPDATE mediaConvertQueue
           SET status='{}'
         WHERE id={}""".format(status, entryID)
    execute_mySQL(query, entryID)


# Split Video into parts
def splitVideo(entry, seconds):
    # Split file extension from filename
    fileName, _, extension = entry["fileName"].rpartition(".")

    command = ["ffmpeg", "-i", entry["filePath"] + entry["fileName"],
               "-c", "copy",
               "-map", "0",
               "-segment_time", str(int(seconds)),
               "-f", "segment",
               "{}{}_part_%03d.{}".format(entry["filePath"],
                                          fileName, extension)]

    # Split the video into segments roughly that length
    info = subprocess.Popen(command)
    # out, err, pid = executeCommand(
    #     ("ffmpeg -i {0}{1}{2} -c copy -map 0 -segment_time {3} -f segment"
    #      " {0}{1}_part_%03d{2}").format(
    #      entry["filePath"], fileName, extension, seconds))

    logging.debug(info)

    # Find all the segments that where created and add them to the converter
    partList = glob.glob("{0}{1}*{2}".format(
            entry["filePath"], fileName, extension))
    if len(partList) > 2:
        for part in partList:
            # Add all except original to convert queue
            if part != "{0}{1}".format(entry["filePath"], entry["fileName"]):
                query = """
                INSERT INTO mediaConvertQueue
                            (clientID, deviceID, groupID, filePath, fileName,
                             mode, app_digitalFrame, app_displayAD,
                             app_alerts, app_jukebox)
                     VALUES ({0},'{1}',{2},'{3}','{4}','{5}',{6},{7},{8},{9})
                """.format(
                        entry["clientID"], entry["deviceID"], entry["groupID"],
                        entry["filePath"], part.replace(entry["filePath"], ""),
                        entry["mode"], entry["app_digitalFrame"],
                        entry["app_displayAD"], entry["app_alerts"],
                        entry["app_jukebox"])
                execute_mySQL(query)
            else:  # Delete the original item now that it's split and not used
                os.remove(part)
        return(len(partList)-1)
    else:
        for part in partList:
            # Exclude the original piece
            if part != "{0}{1}".format(entry["filePath"], entry["fileName"]):
                os.remove(part)
        return(1)


def generateSymbolicLink(path, output_dir, first_replacement="uploaded",
                         second_replacement="home/mediapanel",
                         lua_folder="themes"):
    """
    Generate a symbolic link, replacing the "first_replacement" with a
    second replacement; the conjoined path of second_replacement, lua_folder,
    and replace_dir.
    """
    split_path = path.split("/")
    replaced_index = split_path.index(first_replacement)
    replacement_dir = os.path.join(second_replacement, lua_folder, output_dir)
    split_path[replaced_index] = replacement_dir
    os.symlink(path, os.path.join(*split_path))


# Handles the actual formatting
def formatMediaItem(entry, mediaData):
    # Get the device version to pick correct path

    if mediaData["mediaType"] in ("Video", "Audio"):
        segementCount = splitVideo(entry, 1800)
    else:
        segementCount = 1

    if segementCount > 1:
        updateMediaItemStatus(entry["id"], "Segmented", True)
        return(0)

    # print(splitFilePath(entry["fileName"]))

    # Create Previous name
    fileName = os.path.split(entry["fileName"])[1]
    # trash, previousName, extension = splitFilePath(entry["fileName"])
    previousName, _, extension = fileName.rpartition(".")
    # previousName=entry["fileName"]
    # if previousName[len(previousName) - 4:] == "jpeg":
    #    extension = previousName[len(previousName) - 4:]
    #    previousName=previousName[:len(previousName)-4]

    # Get a temp fileName
    logging.debug("%r %r", mediaData, mediaData.get("mediaType"))
    if mediaData["mediaType"] == "Video":
        tmpFile = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        tmpFile.name += ".mp4"
    elif mediaData["mediaType"] == "Image":
        tmp = "/tmp/"+previousName+"_tmp_"+".png"
    else:
        tmp = mediaData["filePath"]

    # Create Conversion command
    if mediaData["mediaType"] == "Video":

        # Apply rotation filter with ffmpeg to flatten rotation; info-beamer
        # doesn't do rotation > 90deg well
        try:
            # is possible to fail if video has no rotation
            rotationCount = int(mediaData["rotation"]) // 90
        except Exception as e:
            rotationCount = 0
            logging.warning("Error calculating rotation: %r", e)

        if rotationCount == 0:
            command = ["/usr/bin/ffmpeg", "-i", mediaData["filePath"],
                       "-acodec", "ac3",  # ac3 audio codec
                       "-vf", "scale=w=-2:h=min(ih\\,720)",  # scaling filter
                       "-y",  # Overwrite output files
                       tmpFile.name]
        else:
            command = ["/usr/bin/ffmpeg", "-i", mediaData["filePath"],
                       "-acodec", "ac3",  # ac3 audio codec
                       "-vf", "scale=w=-2:h=min(ih\\,720)",  # scaling filter
                       "-vf",  # rotation filter
                       ",".join(["transpose=1"] * rotationCount),
                       "-metadata:s:v:0",
                       "rotate=0",
                       "-y",  # Overwrite output files
                       tmpFile.name]

        tmp = tmpFile.name
    elif mediaData["mediaType"] == "Image":
        command = ["/usr/bin/convert", mediaData["filePath"],
                   "-auto-orient",
                   "-resize", "1920x1080",
                   "-format", "png",
                   "-write", "filename", tmp]

    # Do the conversion
    if mediaData["mediaType"] != "Audio":
        logging.debug("%s", command)
        if subprocess.call(command) != 0:
            try:
                os.remove(mediaData["filePath"])
                updateMediaItemStatus(entry["id"], "Conversion_Failed")
            except Exception as e:
                logging.error("Conversion error: %r", e)
                updateMediaItemStatus(entry["id"],
                                      "Conversion_Failed_FileLeft")
            return False

    # Generate Prefix
    if entry["mode"] == "device" or entry["mode"] == "symLink_device":
        specialPrefix = "user_"+mediaData["mediaType"].lower()+"_"
    elif entry["mode"] == "server":
        specialPrefix = "user_"+mediaData["mediaType"].lower()+"_server_"
    else:
        specialPrefix = "group%s_%s_" % (entry["groupID"],
                                         mediaData["mediaType"].lower())
        # specialPrefix="group_"+mediaData["mediaType"].lower()+"_"

    # Set rotation
    rotateString = ""
    if mediaData["mediaType"] == "Video":
        if mediaData["rotation"] == "90":
            rotateString = "-rotate90"
        elif mediaData["rotation"] == "180":
            rotateString = "-rotate180"
        elif mediaData["rotation"] == "270":
            rotateString = "-rotate270"

    # Generate fileNames
    thumbNail = "".join([specialPrefix, "thumb_",
                         previousName.lower().replace(" ", "_"),
                         rotateString]).replace(",", "")
    finished_fname = "".join([specialPrefix,
                              previousName.lower().replace(" ", "_"),
                              rotateString]).replace(",", "")

    # Add file name extensions
    if mediaData["mediaType"] == "Video":
        finished_fname = finished_fname+".mp4"
    elif mediaData["mediaType"] == "Image":
        finished_fname = finished_fname+".png"
    elif mediaData["mediaType"] == "Audio":
        finished_fname = finished_fname+extension
    thumbNail += ".png"

    finished_fname_path = (entry["filePath"]+finished_fname).strip()
    finished_tname_path = (entry["filePath"]+thumbNail).strip()

    # Move the tmp file to the final filename
    command = ["/bin/mv", tmp, finished_fname_path]
    logging.debug("%r", command)
    if subprocess.call(command) != 0:
        updateMediaItemStatus(entry["id"], "Moving_new_File_Failed")
        return False

    # Create thumbnail
    if mediaData["mediaType"] == "Image":
        command = ["/usr/bin/convert", "-thumbnail", "200",
                   finished_fname_path, finished_tname_path]
        if subprocess.call(command) != 0:
            logging.warning("Entry %s: Failed to create thumb from image",
                            entry["id"])
    elif mediaData["mediaType"] == "Video":
        command = ["/usr/bin/ffmpeg", "-i", finished_fname_path,
                   "-s", "00:00:02",  # 2nd second
                   "-vframes", "1",  # grab 1 frame
                   "-s", "200x200",  # output at size 200x200
                   "-y",  # overwrite
                   finished_tname_path]
        if subprocess.call(command) != 0:
            logging.warning("Entry %s: Failed to create thumb from video",
                            entry["id"])

    # chmod File
    try:
        logging.debug("chmodding %s", finished_fname_path)
        os.chmod(finished_fname_path,
                 stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    except OSError as e:
        updateMediaItemStatus(entry["id"], "chmod_on_new_file_failed")
        logging.error("chmod failed: %r", e)
        return False

    # Delete original file
    if mediaData["mediaType"] != "Audio":
        os.remove(mediaData["filePath"])

    count = 1
    # Check if the device can support the file's Size
    if "symLink" in entry["mode"]:
        count += entry["app_digitalFrame"]
        count += entry["app_displayAD"]
        count += entry["app_alerts"]
        count += entry["app_jukebox"]
    canSupportFile = checkStorageLimitation(entry, finished_fname_path, count)
    logging.info("Storage can support filesize: {0}".format(canSupportFile))

    # Create symlinks
    if entry["mode"] == "symLink_device" or entry["mode"] == "symLink_group":
        if entry["app_digitalFrame"] == 1:
            if canSupportFile:
                try:
                    generateSymbolicLink(finished_fname_path, "digitalFrame")
                    generateSymbolicLink(finished_tname_path, "digitalFrame")
                except OSError as e:
                    logging.warning("%s: symlink failed (%r)", entry["id"], e)

        if entry["app_displayAD"] == 1:
            if canSupportFile:
                try:
                    generateSymbolicLink(finished_fname_path, "displayAD")
                    generateSymbolicLink(finished_tname_path, "displayAD")
                except OSError as e:
                    logging.warning("%s: symlink failed (%r)", entry["id"], e)

        if entry["app_alerts"] == 1:
            if canSupportFile:
                try:
                    generateSymbolicLink(finished_fname_path, "alert")
                    generateSymbolicLink(finished_tname_path, "alert")
                except OSError as e:
                    logging.warning("%s: symlink failed (%r)", entry["id"], e)

        if entry["app_jukebox"] == 1 and canSupportFile:
            if canSupportFile:
                try:
                    generateSymbolicLink(finished_fname_path, "jukebox")
                    generateSymbolicLink(finished_tname_path, "alert")
                except OSError as e:
                    logging.warning("%s: symlink failed (%r)", entry["id"], e)

        # Generate displayName
        displayName = previousName
        for item in ["_-_", "_", "-", "  "]:
            displayName = displayName.replace(item, " ")

        # If the displayName id prefixed with 9 numbers remove the prefix
        if len(displayName) > 9:
            preFixed = True
            for I in range(0, 9):
                if displayName[I] >= '0' and displayName[I] <= '9':
                    continue
                else:
                    preFixed = False
                    break
            if preFixed:
                displayName = displayName[9:]

        displayName = displayName.strip()

        # Create entry in file assets
        if entry["groupID"] == "X":
            entry["groupID"] = "0"

        # Get MBs of the new file++
        size = os.path.getsize(finished_fname_path) / (1024 ** 2)
        resolution = 0.01
        MBs = int(size/resolution)*resolution

        # Check if there is already an entry for this file
        query = """
        SELECT id, app_digitalFrame, app_displayAD, app_alerts, app_jukebox
          FROM userAssets
         WHERE fileName='{0}'
           AND deviceID='{1}'
           AND groupID={2}
        """.format(finished_fname, entry["deviceID"], entry["groupID"])
        existingEntries = execute_mySQL(query, entry["id"], True)
        # ::TODO:: redo this section as a UNIQUE constraint using:
        # UNIQUE(fileName, deviceID, groupID)

        # If no entries exist for the file already
        if len(existingEntries) < 1:
            if not canSupportFile:
                entry["app_digitalFrame"] = 0
                entry["app_displayAD"] = 0
                entry["app_alerts"] = 0
                entry["app_jukebox"] = 0

            query = """
            INSERT INTO userAssets (deviceID, groupID, fileName, fileType,
                                    displayName, clientID, thumbName,
                                    app_digitalFrame, app_displayAD,
                                    app_alerts, app_jukebox, MB)
                 VALUES ('{0}', {1}, '{2}', '{3}', '{4}', {5}, '{6}', {7}, {8},
                         {9}, {10}, {11})
            """.format(
                    entry["deviceID"], entry["groupID"], finished_fname,
                    mediaData["mediaType"], displayName, entry["clientID"],
                    thumbNail, entry["app_digitalFrame"],
                    entry["app_displayAD"], entry["app_alerts"],
                    entry["app_jukebox"], MBs)
            execute_mySQL(query, entry.get("mediaID", entry["id"]))

        # If entries exist, delete duplicates and update original
        else:
            # Delete all duplicate entries
            query = """
            DELETE FROM userAssets
                  WHERE id <> {0}
                    AND fileName='{1}'
                    AND deviceID='{2}'
                    AND groupID={3}
            """.format(existingEntries[0]["id"], entry["deviceID"],
                       entry["groupID"], finished_fname)
            execute_mySQL(query, entry.get("mediaID", entry["id"]), False)

            if not canSupportFile:
                if existingEntries[0]["app_digitalFrame"] == 0:
                    entry["app_digitalFrame"] = 0
                if existingEntries[0]["app_displayAD"] == 0:
                    entry["app_displayAD"] = 0
                if existingEntries[0]["app_alerts"] == 0:
                    entry["app_alerts"] = 0
                if existingEntries[0]["app_jukebox"] == 0:
                    entry["app_jukebox"] = 0

            # Update the Saved entry
            query = """
            UPDATE userAssets
               SET app_digitalFrame={}, app_displayAD={}, app_alerts={},
                   app_jukebox={}, displayName='{}', MB={}
             WHERE id={}
            """.format(entry["app_digitalFrame"], entry["app_displayAD"],
                       entry["app_alerts"], entry["app_jukebox"],
                       existingEntries[0]["id"], displayName, MBs)
            execute_mySQL(query, entry["id"])

    # Update the conversion status
    if canSupportFile:
        updateMediaItemStatus(entry["id"], "Converted", True)
    else:
        updateMediaItemStatus(entry["id"], "Converted_diskFull", True)

    # Tell the devices to get the new content
    if entry["mode"] == "device" or entry["mode"] == "symLink_device":
        query = "UPDATE devices SET updateContent=1 WHERE deviceID='{}'"
        execute_mySQL(query.format(entry["deviceID"]), entry["id"])
    elif entry["mode"] == "symLink_group" or entry["mode"] == "group":
        query = "SELECT deviceID FROM device2groups WHERE groupID={}"
        groupID = entry["groupID"]
        for d in execute_mySQL(query.format(groupID), entry["id"], True):
            query = "UPDATE devices SET updateContent=1 WHERE deviceID={}"
            execute_mySQL(query.format(d["deviceID"]), entry["id"])


# Handles the format entry
# ::TODO:: avoid using threadInprogress somehow?
def processFormatEntry(entry, inProgress, lock):
    threadInProgress(inProgress, lock, 1)

    try:
        mediaData = getMediaData(entry["filePath"]+entry["fileName"])
        logging.debug("Entry %s: %r", entry["id"], mediaData)

        entryStatus = checkFileUpdateStatus(mediaData, entry["id"])
        query = "UPDATE mediaConvertQueue SET status='{}' WHERE id={}"
        execute_mySQL(query.format(entryStatus, entry["id"]), entry["id"])

        if entryStatus == "started_conversion":
            if mediaData["mediaType"] == "PDF":
                # Explode pages into directory and tack "_{n}" onto entry["id"]
                # Current file is at mediaData["filePath"]
                directory = "/tmp/%s" % entry["id"]
                try:  # just in case it somehow exists
                    os.mkdir(directory)
                except OSError as e:
                    logging.warning("Unable to make '%s': %r", directory, e)
                    pass
                subprocess.call(["/usr/bin/pdftoppm", mediaData["filePath"],
                                 "-png", directory + "/output"])
                old_entry = entry.copy()
                os.remove(mediaData["filePath"])
                files = filter(lambda x: "output" in x, os.listdir(directory))
                for i, path in enumerate(files):
                    mediaData = {"rotation": "none", "mediaType": "Image",
                                 "filePath": directory + "/" + path}
                    entry["fileName"] = "%s_%s" % (i, old_entry["fileName"])
                    entry["mediaID"] = "%s_%s" % (entry["id"], i)
                    logging.debug("%r", mediaData)
                    formatMediaItem(entry.copy(), mediaData)
            else:
                formatMediaItem(entry, mediaData)
    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            updateMediaItemStatus(entry["id"], "{0}".format(e))
        except Exception as e:
            logging.warning("Entry %s: issue updating: %r", entry["id"], e)
        logging.warning("Entry %s: %r", entry["id"], e)

    threadInProgress(inProgress, lock, 0)


# Main Program
# Settings
runMode = sys.argv[-1]

if runMode == "both":

    downloader = threading.Process(target=urlDownloader, args=(2,))
    mediaFormater = threading.Process(target=mediaFormat, args=(2,))
    downloader.start()
    mediaFormater.start()
    downloader.join()
    mediaFormater.join()

elif runMode == "download":

    query = "SELECT * FROM urlDownloader"
    while len(execute_mySQL(query, -1, True)) < 1:
        sleep(randint(2, 15))

    downloader = threading.Process(target=urlDownloader, args=(5,))
    downloader.start()
    downloader.join()

elif runMode == "convertMP4":

    query = """
      SELECT *
        FROM mediaConvertQueue
       WHERE status IS NULL
         AND convertTimestamp IS NULL
         AND (RIGHT(fileName, 4) = '.mp4' OR RIGHT(fileName, 4) = '.m4v'
              OR RIGHT(fileName, 4) = '.flv' OR RIGHT(fileName, 4) = '.mov')
    ORDER BY uploadTimestamp
    """

    while len(execute_mySQL(query, -1, True)) < 1:
        sleep(randint(2, 15))

    for entry in execute_mySQL(query, -1, True):
        try:
            mediaData = getMediaData(entry["filePath"]+entry["fileName"])
            logging.debug("Entry {0}: {1}".format(entry["id"], mediaData))

            entryStatus = checkFileUpdateStatus(mediaData, entry["id"])
            query = "UPDATE mediaConvertQueue SET status='{}' WHERE id={}"
            execute_mySQL(query.format(entryStatus, entry["id"]), entry["id"])

            if entryStatus == "started_conversion":
                formatMediaItem(entry, mediaData)
        except Exception as e:
            try:
                import traceback
                traceback.print_exc()
                updateMediaItemStatus(entry["id"], "{0}".format(e))
            except Exception as e:
                logging.warning("Entry {0}: {1}".format(entry["id"], e))
            logging.error("Entry {0}: {1}".format(entry["id"], e))

elif runMode == "convert":

    query = """
      SELECT *
        FROM mediaConvertQueue
       WHERE status IS NULL
         AND convertTimestamp IS NULL
         AND RIGHT(fileName, 4) <> '.mp4' AND RIGHT(fileName, 4) <> '.m4v'
         AND RIGHT(fileName, 4) <> '.flv' AND RIGHT(fileName, 4) <> '.mov'
    ORDER BY uploadTimestamp
    """

    while len(execute_mySQL(query, -1, True)) < 1:
        sleep(randint(2, 15))

    sleep(1)
    while len(execute_mySQL(query, -1, True)) >= 1:
        mediaFormater = threading.Process(target=mediaFormat, args=(8, True))
        mediaFormater.start()
        mediaFormater.join()
        sleep(1)
        del mediaFormater
        sleep(1)

sys.exit(0)
