import { WebSocket } from 'ws';
import { decode } from 'cborg';

// Connect to the local sidecar firehose (default port is 8089 in .env.local, fallback to 8080)
const port = process.env.PORT || 8080;
const url = `ws://localhost:${port}/xrpc/com.atproto.sync.subscribeRepos`;

console.log(`Connecting to local ATProto firehose at: ${url}`);
const ws = new WebSocket(url);

ws.on('open', () => {
  console.log('\n=============================================================');
  console.log(' SUCCESS: Connected to local ATProto firehose!');
  console.log(' Listening for commits... (Make a post to trigger a commit)');
  console.log('=============================================================\n');
});

ws.on('message', (data) => {
  try {
    console.log(`\n[Firehose Frame] Received binary frame of ${data.length} bytes`);
    
    // ATProto frames contain concatenated CBOR blocks.
    // The first block is the header, indicating the operation type.
    const header = decode(data);
    console.log('  Header:', JSON.stringify(header));
    
    // We can check if it is a commit frame
    if (header.t === '#commit' || header.op === 1) {
      console.log('  Type  : Repository Commit');
    }
  } catch (err) {
    console.log('  Frame data (non-decoded):', data.slice(0, 100).toString('hex') + '...');
  }
});

ws.on('error', (err) => {
  console.error('WebSocket Error:', err.message);
  console.log('Make sure the Fedify sidecar is running locally on the correct port.');
});

ws.on('close', () => {
  console.log('Firehose connection closed.');
});
