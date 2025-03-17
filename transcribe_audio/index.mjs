import fetch from 'node-fetch';
import FormData from 'form-data';
import mp3Duration from 'mp3-duration';

const API_KEY = process.env.OPENAI_API_KEY;

// Function to slice the audio blob into two parts
// const sliceBlob = async (audioBlob, startTimeInSeconds, endTimeInSeconds) => {
//     const startBytes = startTimeInSeconds * 1000;  // Convert seconds to milliseconds
//     // console.log("start => ",startBytes);
//     const endBytes = endTimeInSeconds * 1000;      // Convert seconds to milliseconds
//     // console.log("end => ",endBytes);
//     return audioBlob.slice(startBytes, endBytes);  // Slice from start to end in bytes
//   };
// function sliceBlob(blob, offset, size) {
//     // Ensure the offset and size are within the blob boundaries
//     const end = Math.min(offset + size, blob.size);
//     // Calculate the duration of the chunk
//     const chunk = blob.slice(offset, end);
//     const chunkSize = chunk.size;
//     const durationInSeconds = chunkSize / 12000;
//     console.log(`Chunk starting at offset ${offset} has a duration of ${durationInSeconds.toFixed(2)} seconds`);

//     return chunk;
// }


// Handle transcription
const handleTranscription = async (audioBlob, partLabel, prompt, language) => {
  const formData = new FormData();
  formData.append("file", Buffer.from(await audioBlob.arrayBuffer()), `${partLabel}_audio_snippet.mp3`);
  formData.append("model", "whisper-1");
  formData.append("response_format", "srt");
  if(language != false){
    formData.append("language","nl");
  }
  if(prompt != null){
    formData.append("prompt", prompt);
  }

  const openaiResponse = await fetch('https://api.openai.com/v1/audio/transcriptions', {
      method: 'POST',
      headers: {
          'Authorization': `Bearer ${API_KEY}`,
      },
      body: formData
  });

  if (!openaiResponse.ok) {
      const errorText = await openaiResponse.text();
      throw new Error(`OpenAI API response for ${partLabel} was not ok: ${openaiResponse.status} - ${errorText}`);
  }

  return await openaiResponse.text();
};

// Function to split the audio blob into chunks based on specified sizes
async function splitBlob(blob, subsequentChunkSize, initialChunkSize) {
    const chunks = [];
    let offset = 0;

    // Add the initial chunk of specified size
    if (initialChunkSize && offset < blob.size) {
        chunks.push(blob.slice(offset, offset + initialChunkSize));
        offset += initialChunkSize;
    }

    // Add subsequent chunks of the specified size
    while (offset < blob.size) {
        chunks.push(blob.slice(offset, offset + subsequentChunkSize));
        offset += subsequentChunkSize;
    }

    return chunks;
}

function mergeTranscripts(transcripts) {
    function parseTranscript(transcript) {
        const lines = transcript.trim().split("\n");
        const parsed = [];
        let id = null, timestamp = null, value = "";

        lines.forEach(line => {
            if (/^\d+$/.test(line.trim())) {
                if (id !== null) {
                    parsed.push({ id, timestamp, value: value.trim() });
                }
                id = parseInt(line.trim(), 10);
                value = "";
            } else if (/-->/g.test(line)) {
                timestamp = line.trim();
            } else {
                value += line.trim() + " ";
            }
        });

        if (id !== null) {
            parsed.push({ id, timestamp, value: value.trim() });
        }

        return parsed;
    }

    function adjustTimestamps(baseEnd, ts) {
        const timeToMs = ts => {
            const [hh, mm, ss] = ts.split(':').map(part => parseFloat(part.replace(',', '.')));
            return ((hh * 3600) + (mm * 60) + ss) * 1000;
        };

        const msToTime = ms => {
            const hh = String(Math.floor(ms / 3600000)).padStart(2, '0');
            ms %= 3600000;
            const mm = String(Math.floor(ms / 60000)).padStart(2, '0');
            ms %= 60000;
            const ss = (ms / 1000).toFixed(3).replace('.', ',').padStart(6, '0');
            return `${hh}:${mm}:${ss}`;
        };

        const [start, end] = ts.split(" --> ").map(timeToMs);
        const adjustedStart = baseEnd + start;
        const adjustedEnd = baseEnd + end;

        return `${msToTime(adjustedStart)} --> ${msToTime(adjustedEnd)}`;
    }

    let merged = parseTranscript(transcripts[0]);
    let baseEnd = merged[merged.length - 1].timestamp.split(" --> ")[1];
    baseEnd = baseEnd.split(':').reduce((ms, t) => ms * 60 + parseFloat(t.replace(',', '.')), 0) * 1000; // Convert to ms

    for (let tIndex = 1; tIndex < transcripts.length; tIndex++) {
        const parsed = parseTranscript(transcripts[tIndex]);
        for (let i = 0; i < parsed.length; i++) {
            const newId = merged.length + 1;
            const adjustedTimestamp = adjustTimestamps(baseEnd, parsed[i].timestamp);
            merged.push({
                id: newId,
                timestamp: adjustedTimestamp,
                value: parsed[i].value
            });
        }

        // Update baseEnd to the new last segment's end timestamp
        const lastSegmentEnd = merged[merged.length - 1].timestamp.split(" --> ")[1];
        baseEnd = lastSegmentEnd
            .split(':')
            .reduce((ms, t) => ms * 60 + parseFloat(t.replace(',', '.')), 0) * 1000; // Convert to ms
    }

    return merged;
}

// function mergeTranscripts(transcripts) {
//   function parseTranscript(transcript) {
//         const lines = transcript.trim().split("\n");
//         const parsed = [];
//         let id = null, timestamp = null, value = "";

//         lines.forEach(line => {
//             if (/^\d+$/.test(line.trim())) {
//                 if (id !== null) {
//                     parsed.push({ id, timestamp, value: value.trim() });
//                 }
//                 id = parseInt(line.trim(), 10);
//                 value = "";
//             } else if (/-->/g.test(line)) {
//                 timestamp = line.trim();
//             } else {
//                 value += line.trim() + " ";
//             }
//         });

//         if (id !== null) {
//             parsed.push({ id, timestamp, value: value.trim() });
//         }

//         return parsed;
//     }

//   function addTimestamps(ts1, ts2) {
//       const timeToMs = ts => {
//           const [hh, mm, ss] = ts.split(':').map(part => parseFloat(part.replace(',', '.')));
//           return ((hh * 3600) + (mm * 60) + ss) * 1000;
//       };

//       const msToTime = ms => {
//           const hh = String(Math.floor(ms / 3600000)).padStart(2, '0');
//           ms %= 3600000;
//           const mm = String(Math.floor(ms / 60000)).padStart(2, '0');
//           ms %= 60000;
//           const ss = (ms / 1000).toFixed(3).replace('.', ',').padStart(6, '0');
//           return `${hh}:${mm}:${ss}`;
//       };

//       const [start1, end1] = ts1.split(" --> ").map(timeToMs);
//       const [start2, end2] = ts2.split(" --> ").map(timeToMs);
//       const offset = end1 - start1;

//       const newStart = msToTime(end1);
//       const newEnd = msToTime(end1 + (end2 - start2));
//       return `${newStart} --> ${newEnd}`;
//   }

// //   const parsed1 = parseTranscript(transcript1);
// //   const parsed2 = parseTranscript(transcript2);

// //   let merged = [...parsed1];

// //   for (let i = 0; i < parsed2.length; i++) {
// //       const newId = merged.length + 1;
// //       const adjustedTimestamp = addTimestamps(
// //           merged[merged.length - 1].timestamp,
// //           parsed2[i].timestamp
// //       );
// //       merged.push({
// //           id: newId,
// //           timestamp: adjustedTimestamp,
// //           value: parsed2[i].value
// //       });
// //   }

// //   return merged;
//     // Initialize merged array with parsed transcripts from the first one
//     let merged = parseTranscript(transcripts[0]);

//     // Iterate over remaining transcripts to merge them
//     for (let tIndex = 1; tIndex < transcripts.length; tIndex++) {
//         const parsed = parseTranscript(transcripts[tIndex]);
        
//         for (let i = 0; i < parsed.length; i++) {
//             const newId = merged.length + 1;
//             const adjustedTimestamp = addTimestamps(
//                 merged[merged.length - 1].timestamp,
//                 parsed[i].timestamp
//             );
//             merged.push({
//                 id: newId,
//                 timestamp: adjustedTimestamp,
//                 value: parsed[i].value
//             });
//         }
//     }

//     return merged;
// }

async function getMp3Duration(blob) {
    try {
      // Convert Blob to ArrayBuffer
      const arrayBuffer = await blob.arrayBuffer();
      
      // Initialize the AudioContext for Node.js environment
      const audioContext = new AudioContext();
      const audioBuffer = await audioContext.decodeAudioData(arrayBuffer);
      
      console.log('Duration: ' + audioBuffer.duration + ' seconds');
      return audioBuffer.duration; // Return the duration if needed
    } catch (error) {
      console.log('Error decoding MP3 file: ' + error);
    }
  }

// Function to calculate bytes per minute from audioBlob
async function getMP3BytesPerMinute(audioBlob, duration) {
    // Get the file size in bytes
    const fileSize = audioBlob.size;

    // Create an Audio element to load the MP3 and extract its duration
    // const audio = new Audio();
    // audio.src = URL.createObjectURL(audioBlob);

    // // Wait until the metadata is loaded to get the duration
    // await new Promise((resolve, reject) => {
    //     audio.onloadedmetadata = resolve;  // Resolves when metadata is loaded
    //     audio.onerror = reject;           // Rejects if there's an error
    // });

    // Duration in seconds from the audio element
    const durationInSeconds = duration;

    // Ensure the duration is valid
    if (durationInSeconds > 0) {
        // Convert duration to minutes
        const durationInMinutes = durationInSeconds / 60;

        // Calculate bytes per minute
        const bytesPerMinute = parseInt(fileSize / durationInMinutes);
        return bytesPerMinute;
    } else {
        throw new Error('Invalid audio duration.');
    }
}

export const handler = async (event) => {
  // Set the CORS headers
  const headers = {
        'Access-Control-Allow-Origin': '*', // Allow all origins (or specify the exact origin)
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE', // Allowed HTTP methods
        'Access-Control-Allow-Headers': 'Content-Type, Authorization' // Allowed headers
    };
  try {
    console.log("event body => ",JSON.parse(event.body));
    const body = JSON.parse(event.body);
    const url = body["url"];
    const prompt = body["prompt"];
    const language = body["language"];
    console.log("url => ",url);
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Failed to fetch audio file: ${response.statusText}`);
    const audioBlob = await response.blob();
    
    // Fetch the MP3 file as a Buffer for processing
    const bufferResponse = await fetch(url); // Re-fetch the URL to get the buffer
    const buffer = await bufferResponse.buffer();
    // Return a Promise that resolves with the duration
    const duration = await new Promise((resolve, reject) => {
      mp3Duration(buffer, (err, duration) => {
        if (err) {
          reject('Error decoding MP3 file: ' + err);
        } else {
          resolve(duration);  // Resolves the duration
        }
      });
    });

    // Save the duration in a variable
    console.log('Duration:', duration, 'seconds');

    const bps = await getMP3BytesPerMinute(audioBlob, duration);
    console.log('Bytes per minute:', bps);

    // Define sizes in bytes (adjust these values based on actual byte-per-second rate)
    const initialChunkSize = 3 * bps; // 3 minutes in bytes
    const subsequentChunkSize = 10 * bps; // 25 minutes in bytes

    // Calculate the duration of the audio in seconds
    const audioDurationInSeconds = audioBlob.size / 12000; // You may need to adjust this depending on the audio-to-byte relation
    console.log("diff = ",audioBlob.size,initialChunkSize)
    // Declare mergedTranscript outside the if/else block
    let mergedTranscript;

    // Check if the audio is longer than the initial chunk size
    if (audioBlob.size > initialChunkSize) {
        // Split the blob: first chunk is 3 minutes, subsequent chunks are 10 minutes
        const chunks = await splitBlob(audioBlob, subsequentChunkSize, initialChunkSize);
        console.log("Number of chunks:", chunks.length);

        // Transcribe all chunks concurrently
        const transcriptions = await Promise.all(
            chunks.map((chunk, index) =>
                handleTranscription(chunk, `Part ${index + 1}`, prompt, language)
            )
        );

        // Log individual transcriptions
        transcriptions.forEach((transcription, index) => {
            console.log(`Transcript Part ${index + 1}:`, transcription);
        });

        // Merge all transcriptions into a single transcript
        mergedTranscript = mergeTranscripts(transcriptions);

        // Return the final merged transcription
        console.log("Final Transcript:", mergedTranscript);

    }else {
        // Handle the case where the audio is within the initial chunk size
        console.log("Audio length is within the initial chunk size.");
    
        // Transcribe the audio blob directly
        const transcription = await handleTranscription(audioBlob, "Full Audio", prompt, language);
    
        // Log the transcription
        console.log("Final Transcript:", transcription);

    
        // Set the merged transcript as the direct transcription
        mergedTranscript = mergeTranscripts([transcription]);
    }

    // Return the final transcription as a response
    return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ transcription: mergedTranscript }),
    };


  } catch (error) {
    console.error('Handler Error:', error.message);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ message: 'Transcription failed', error: error.message }),
      statusText: error.message
    };
  }
};