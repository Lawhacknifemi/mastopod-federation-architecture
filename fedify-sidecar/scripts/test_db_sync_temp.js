import pg from 'pg';
import { randomUUID } from 'crypto';
const { Client } = pg;

async function run() {
  const client = new Client({
    connectionString: 'postgresql://sidecar_user:sidecar_password@localhost:5432/fedify_sidecar'
  });
  
  await client.connect();
  
  const postId = randomUUID();
  console.log('Inserting test post with fresh ID:', postId);
  
  await client.query(
    `INSERT INTO local_cache_posts (
      id, actor_uri, content, post_type, name, summary, hashtags, attachments, synced_to_fuseki, sync_attempts
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, 0)`,
    [
      postId,
      'http://localhost:3000/testuser',
      'This is a fresh new post with a completely unique UUID! ' + postId,
      'note',
      null,
      null,
      ['test'],
      JSON.stringify([])
    ]
  );
  
  console.log('Test post inserted. Waiting 6 seconds for sync worker...');
  await new Promise(resolve => setTimeout(resolve, 6000));
  
  const { rows } = await client.query(
    'SELECT id, synced_to_fuseki, sync_attempts, error_message FROM local_cache_posts WHERE id = $1',
    [postId]
  );
  
  console.log('Result:', rows[0]);
  
  await client.end();
}

run().catch(console.error);
