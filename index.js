import { dirname, join } from 'path';
import { promisify } from 'util';
import { promises, createReadStream, createWriteStream } from 'fs';
import { pipeline, Transform } from 'stream';
import debug from 'debug';
import csvToJson from 'csvtojson';
import jsonToCsv from 'json-to-csv-stream';
import StreamConcat from 'stream-concat';

const pipelineAsync = promisify(pipeline);
const log = debug('app:concat');
const { readdir } = promises;
const { pathname: currentFile } = new URL(import.meta.url);
const cwd = dirname(currentFile);
const filesDir = `${cwd}/dataset`;
const output = `${cwd}/final.csv`;

console.time('concat-data');
const files = (await readdir(filesDir))
      .filter(item => !(!!~item.indexOf('.zip')));

log(`processing ${files}`);

const ONE_SECOND = 1000;
// QUANDO OS OUTROS PROCESSOS ACABAREM ELE MORRE JUNTO
setInterval(() => {
  process.stdout.write('.');
}, ONE_SECOND).unref();

const streams = files.map(
  item => createReadStream(join(filesDir, item))
);
const combinedStreams = new StreamConcat(streams)
const finalStream = createWriteStream(output);
const handleStream = new Transform({
  transform: (chunk, encoding, cb) => {
    const data = JSON.parse(chunk);
    const ouput = {
      id: data.Respondent,
      country: data.Country,
    };

    log(`id: ${output.id}`);
    
    return cb(null, JSON.stringify(ouput)); 
  },
});

await pipelineAsync(
  combinedStreams,
  csvToJson(),
  handleStream,
  jsonToCsv(),
  finalStream,
);

log(`${files.length} files merged! on ${output}`);
console.timeEnd('concat-data');
