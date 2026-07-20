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
      'This is a single manual sync test! ' + postId,
      'note',
      null,
      null,
      ['test'],
      JSON.stringify([])
    ]
  );
  
  // Query it back to match the row format
  const { rows } = await client.query('SELECT * FROM local_cache_posts WHERE id = $1', [postId]);
  const row = rows[0];
  
  console.log('Performing manual sync...');
  const activityPodsUrl = 'http://localhost:3000';
  const activityPodsToken = 'local-dev-token';
  const webId = row.actor_uri;
  const username = webId.split("/").pop() || "user";
  const objectUri = `${activityPodsUrl}/${username}/data/note/${row.id}`;

  const tags = (row.hashtags || []).map((tag) => ({
    type: "Hashtag",
    name: tag.startsWith("#") ? tag : `#${tag}`,
    href: `${activityPodsUrl}/tags/${tag.replace(/^#/, "")}`,
  }));

  const activity = {
    "@context": "https://www.w3.org/ns/activitystreams",
    type: "Create",
    actor: webId,
    to: ["https://www.w3.org/ns/activitystreams#Public"],
    object: {
      "@id": objectUri,
      type: "Note",
      attributedTo: webId,
      content: row.content || "",
      to: ["https://www.w3.org/ns/activitystreams#Public"],
      tag: tags
    },
  };

  try {
    const response = await fetch(`${activityPodsUrl}/api/internal/social/outbox`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${activityPodsToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ actorUri: webId, activity }),
    });

    const status = response.status;
    const text = await response.text();
    console.log('Response Status:', status);
    console.log('Response Text:', text);
  } catch (err) {
    console.error('Fetch Error:', err);
  }

  await client.end();
}

run().catch(console.error);
