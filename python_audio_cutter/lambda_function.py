import os
import math
import uuid
import json
import requests
from pydub import AudioSegment
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

API_KEY = os.getenv("OPENAI_API_KEY")

def split_mp3(input_file, initial_chunk_duration=3 * 60 * 1000, subsequent_chunk_duration=10 * 60 * 1000):
    try:
        print(f"Splitting audio file: {input_file}")
        song = AudioSegment.from_mp3(input_file)
        exported_files = []

        # Define export parameters
        export_params = {"format": "mp3",
            "parameters": ["-ar", "44100", "-ab", "128k", "-ac", "2"]
        }

        # First chunk (initial duration)
        first_chunk = song[:initial_chunk_duration]
        output_file = f"/tmp/{uuid.uuid4()}_part1.mp3"
        first_chunk.export(output_file, **export_params)
        exported_files.append(output_file)
        print(f"Exported first chunk: {output_file}")

        # Remaining chunks (subsequent duration)
        total_duration = len(song)
        num_chunks = math.ceil((total_duration - initial_chunk_duration) / subsequent_chunk_duration)

        for i in range(num_chunks):
            start = initial_chunk_duration + i * subsequent_chunk_duration
            end = min(start + subsequent_chunk_duration, total_duration)
            chunk = song[start:end]
            output_file = f"/tmp/{uuid.uuid4()}_part{i + 2}.mp3"
            chunk.export(output_file, **export_params)
            exported_files.append(output_file)
            print(f"Exported chunk {i + 2}: {output_file}")

        return exported_files
    except Exception as e:
        raise Exception(f"Error during audio splitting: {str(e)}")

def merge_transcripts(transcripts):
    try:
        print("Merging transcripts...")
        def parse_transcript(transcript):
            lines = transcript.strip().split("\n")
            parsed = []
            id_ = None
            timestamp = None
            value = ""

            for line in lines:
                line = line.strip()
                if line.isdigit():
                    if id_ is not None:
                        parsed.append({"id": id_, "timestamp": timestamp, "value": value.strip()})
                    id_ = int(line)
                    value = ""
                elif "-->" in line:
                    timestamp = line
                else:
                    value += line + " "

            if id_ is not None:
                parsed.append({"id": id_, "timestamp": timestamp, "value": value.strip()})

            return parsed

        def adjust_timestamps(base_end, ts):
            def time_to_ms(ts):
                hh, mm, ss = map(float, ts.replace(",", ".").split(":"))
                return int((hh * 3600 + mm * 60 + ss) * 1000)

            def ms_to_time(ms):
                hh = ms // 3600000
                ms %= 3600000
                mm = ms // 60000
                ms %= 60000
                ss = ms / 1000
                return f"{int(hh):02}:{int(mm):02}:{ss:06.3f}".replace(".", ",")

            start, end = map(time_to_ms, ts.split(" --> "))
            adjusted_start = base_end + start
            adjusted_end = base_end + end
            return f"{ms_to_time(adjusted_start)} --> {ms_to_time(adjusted_end)}"

        merged = parse_transcript(transcripts[0])
        base_end = merged[-1]["timestamp"].split(" --> ")[1]
        base_end = sum(float(x.replace(",", ".")) * 60 ** i for i, x in enumerate(reversed(base_end.split(":")))) * 1000

        for transcript in transcripts[1:]:
            parsed = parse_transcript(transcript)
            for entry in parsed:
                new_id = len(merged) + 1
                adjusted_timestamp = adjust_timestamps(base_end, entry["timestamp"])
                merged.append({
                    "id": new_id,
                    "timestamp": adjusted_timestamp,
                    "value": entry["value"]
                })

            last_segment_end = merged[-1]["timestamp"].split(" --> ")[1]
            base_end = sum(float(x.replace(",", ".")) * 60 ** i for i, x in enumerate(reversed(last_segment_end.split(":")))) * 1000

        print("Transcripts merged successfully.")
        return merged
    except Exception as e:
        raise Exception(f"Error during merging transcripts: {str(e)}")
        
# (Keep your `split_mp3` and `merge_transcripts` functions unchanged)


def handle_transcription(audio_stream, part_label, prompt=None, language=None):
    try:
        print(f"Transcribing {part_label}...", prompt)
        url = "https://api.openai.com/v1/audio/transcriptions"
        headers = {"Authorization": f"Bearer {API_KEY}"}
        files = {"file": audio_stream}
        data = {
            "model": "whisper-1",
            "response_format": "srt",
        }
        if prompt:
            data["prompt"] = prompt
        if language:
            data["language"] = "nl"

        response = requests.post(url, headers=headers, files=files, data=data)
        if response.status_code != 200:
            raise Exception(f"Failed transcription for {part_label}: {response.status_code} - {response.text}")
        
        print(f"Successfully transcribed {part_label}")
        try:
            print("response json ::", response)
        except Exception as e:
            raise Exception(f"Error during transcription of json response.: {str(e)}")
        return response.text
    except Exception as e:
        raise Exception(f"Error during transcription of {part_label}: {str(e)}")


def transcribe_chunks_in_parallel(chunks, prompt=None, language=None):
    try:
        print("Starting parallel transcription...")
        # A dictionary to maintain the sequence
        transcriptions = [None] * len(chunks)

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor() as executor:
            future_to_index = {
                executor.submit(handle_transcription, open(chunk, "rb"), f"Part {index + 1}", prompt, language): index
                for index, chunk in enumerate(chunks)
            }

            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    transcriptions[index] = future.result()
                except Exception as exc:
                    print(f"Chunk {index + 1} generated an exception: {exc}")
                    raise

        print("Parallel transcription completed.")
        return transcriptions
    except Exception as e:
        raise Exception(f"Error during parallel transcription: {str(e)}")



def upload_chunks_to_s3(chunks, bucket_name, s3_folder="audio_chunks"):
    """
    Uploads the split .mp3 files to a specified AWS S3 bucket.

    :param chunks: List of file paths for the audio chunks to upload.
    :param bucket_name: Name of the S3 bucket.
    :param s3_folder: Folder within the S3 bucket to store the chunks. Defaults to 'audio_chunks'.
    :return: List of S3 URLs for the uploaded chunks.
    """
    try:
        print("Uploading chunks to S3...")
        s3_client = boto3.client('s3')
        s3_urls = []

        for chunk in chunks:
            # Extract the file name from the chunk path
            file_name = os.path.basename(chunk)

            # Create the S3 key (path in the bucket)
            s3_key = f"{s3_folder}/{file_name}"

            # Upload the file to S3
            s3_client.upload_file(chunk, bucket_name, s3_key)

            # Construct the S3 file URL (assuming public access or default URL structure)
            s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
            s3_urls.append(s3_url)

            print(f"Uploaded {chunk} to {s3_url}")

        print("All chunks uploaded successfully.")
        return s3_urls

    except NoCredentialsError:
        raise Exception("AWS credentials not found. Make sure they're configured in your environment.")
    except ClientError as e:
        raise Exception(f"AWS ClientError: {str(e)}")
    except Exception as e:
        raise Exception(f"Error during S3 upload: {str(e)}")


def lambda_handler(event, context):
    try:
        print("Processing lambda event...")
        body = json.loads(event.get("body", "{}"))
        audio_url = body.get("url")
        prompt = body.get("prompt", None)
        language = body.get("language", None)
        bucket_name = "transcriber-analysis"  # Add bucket name to the input JSON

        if not audio_url:
            raise ValueError("Audio URL is required")

        print(f"Downloading audio from URL: {audio_url}")
        response = requests.get(audio_url)
        if response.status_code != 200:
            raise Exception(f"Failed to download audio: {response.status_code}")
        
        input_file = f"/tmp/{uuid.uuid4()}.mp3"
        with open(input_file, "wb") as f:
            f.write(response.content)
        print(f"Downloaded audio to: {input_file}")

        # Split the audio file into chunks
        chunks = split_mp3(input_file)
        

        # Upload the chunks to S3
        #s3_urls = upload_chunks_to_s3(chunks, bucket_name, s3_folder="demo")
        #print("Chunks uploaded to S3:", s3_urls)


        # Transcribe each chunk in parallel
        transcriptions = transcribe_chunks_in_parallel(chunks, prompt, language)

        # Merge the transcripts
        print(f"Before merge transcriptions : ",transcriptions)
        final_transcript = merge_transcripts(transcriptions)

        # Clean up temporary files
        for chunk in chunks:
            os.remove(chunk)
        os.remove(input_file)
        print("Cleaned up temporary files.")

        return {
            "statusCode": 200,
            "body": json.dumps({"transcription": final_transcript})
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
