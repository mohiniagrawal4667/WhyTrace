const path = require('node:path');
const fs = require('node:fs');

// Ensure .env.local is loaded even when lib/neo4j.js is imported directly
try {
  const dotenv = require('dotenv');
  const candidateDirs = [
    path.resolve(__dirname, '..'),
    process.cwd(),
    path.resolve(__dirname, '..', '..'),
    path.resolve(process.cwd(), 'WhyTrace-Delivery'),
    path.resolve(process.cwd(), 'WhyTrace-Delivery', 'WhyTrace-Delivery')
  ];
  for (const dir of [...new Set(candidateDirs)]) {
    const pLocal = path.resolve(dir, '.env.local');
    if (fs.existsSync(pLocal)) dotenv.config({ path: pLocal, override: true });
    const pEnv = path.resolve(dir, '.env');
    if (fs.existsSync(pEnv)) dotenv.config({ path: pEnv });
  }
} catch {}

function cleanVal(v) {
  if (typeof v !== 'string') return '';
  let s = v.trim();
  if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
    s = s.slice(1, -1).trim();
  }
  return s;
}

function getNeo4jConfig() {
  const uri = cleanVal(process.env.NEO4J_URI || process.env.NEO4J_URL);
  const username = cleanVal(process.env.NEO4J_USERNAME || process.env.NEO4J_USER);
  const password = cleanVal(process.env.NEO4J_PASSWORD || process.env.NEO4J_PASS);
  return { uri, username, password };
}

function validateConfiguration() {
  const { uri, username, password } = getNeo4jConfig();
  const missing = [];
  if (!uri) missing.push('NEO4J_URI');
  if (!username) missing.push('NEO4J_USERNAME');
  if (!password) missing.push('NEO4J_PASSWORD');

  if (missing.length > 0) {
    return {
      configured: false,
      message: `Missing required Neo4j credentials in .env.local: ${missing.join(', ')}.`
    };
  }

  if (/[<>]/.test(uri) || /[<>]/.test(username) || /[<>]/.test(password)) {
    return {
      configured: false,
      message: 'Placeholder angle brackets (<...>) detected in Neo4j credentials in .env.local. Replace placeholders with real AuraDB connection values.'
    };
  }

  if (!/^(neo4j(\+s|\+ssc)?|bolt(\+s|\+ssc)?):\/\//i.test(uri)) {
    return {
      configured: false,
      message: 'Invalid NEO4J_URI protocol. Expected a valid connection URI starting with neo4j+s://, neo4j://, or bolt://.'
    };
  }

  return { configured: true, message: 'Configuration valid.' };
}

let driverPromise;
function isConfigured() {
  return validateConfiguration().configured;
}

async function getDriver() {
  const validation = validateConfiguration();
  if (!validation.configured) throw new Error(validation.message);
  if (!driverPromise) driverPromise = (async () => {
    let neo4j;
    try { neo4j = require('neo4j-driver'); }
    catch {
      try {
        const imported = await import('neo4j-driver');
        neo4j = imported.default || imported;
      } catch {
        throw new Error('neo4j-driver is not installed. Run: npm install neo4j-driver');
      }
    }
    const { uri, username, password } = getNeo4jConfig();
    return neo4j.driver(uri, neo4j.auth.basic(username, password), { maxConnectionPoolSize: 10, connectionAcquisitionTimeout: 10000 });
  })();
  return driverPromise;
}

function safeError(error) {
  return String(error?.message || 'Unknown Neo4j error')
    .replace(/neo4j(?:\+s|\+ssc)?:\/\/[^\s'"]+/gi, '[database URI]')
    .replace(/bolt(?:\+s|\+ssc)?:\/\/[^\s'"]+/gi, '[database URI]')
    .replace(/password[^\s'"]*/gi, '[redacted]');
}

async function checkNeo4j() {
  const validation = validateConfiguration();
  if (!validation.configured) {
    return { configured: false, connected: false, persistence: false, message: validation.message };
  }
  try {
    const driver = await getDriver();
    await driver.verifyConnectivity();
    const session = driver.session();
    try {
      const result = await session.run('RETURN 1 AS result');
      const val = Number(result.records[0]?.get('result'));
      if (val !== 1) {
        return { configured: true, connected: false, persistence: false, message: `Query verification returned unexpected value: ${val}` };
      }
      return { configured: true, connected: true, persistence: false, message: 'AuraDB connection verified. Persistence is verified after an actual write/read-back.' };
    } finally {
      await session.close();
    }
  } catch (error) {
    await closeNeo4j();
    return { configured: true, connected: false, persistence: false, message: safeError(error) };
  }
}
async function persistGraph(graph) {
  const driver = await getDriver();
  const repoId = `repository:${graph.repository.fullName}`;
  // IDs are scoped to a repository in Neo4j. A file named src/config.ts or
  // the same contributor appearing in two repositories must not collide.
  const dbId = sourceId => `${repoId}::${sourceId}`;
  const records = [
    ...graph.nodes.map(n => ({
      id: dbId(n.id),
      props: {
        id: dbId(n.id), sourceId: n.id, repoId, kind: n.kind, label: n.label || '',
        subtitle: n.subtitle || '', url: n.url || '', date: n.date || '', author: n.author || '',
        body: n.body || '', preview: n.preview || '', metadata: JSON.stringify(n.metadata || {}),
        confidence: typeof n.confidence === 'number' ? n.confidence : null
      }
    })),
    ...graph.evidence.map(e => ({
      id: dbId(`evidence:${e.id}`),
      props: {
        id: dbId(`evidence:${e.id}`), sourceId: `evidence:${e.id}`, repoId, kind: 'evidence',
        evidenceId: e.id, evidenceKind: e.kind, title: e.title, body: e.body || '',
        url: e.url || '', date: e.date || '', author: e.author || '', tags: (e.tags || []).filter(tag => typeof tag === 'string'),
        searchText: [e.title || '', e.body || '', ...(e.tags || [])].join(' ').toLowerCase()
      }
    }))
  ];
  const edges = graph.edges.map(e => ({ id: dbId(e.id), sourceId: e.id, from: dbId(e.from), to: dbId(e.to), kind: e.kind, label: e.label || '' }));
  const session = driver.session();
  try {
    await session.run('CREATE CONSTRAINT artifact_id IF NOT EXISTS FOR (a:Artifact) REQUIRE a.id IS UNIQUE');
    return await session.executeWrite(async tx => {
      // Replace only this repository's snapshot; other repositories remain isolated.
      await tx.run('MATCH (a:Artifact {repoId:$repoId}) DETACH DELETE a', { repoId });
      await tx.run('UNWIND $items AS item MERGE (a:Artifact {id:item.id}) SET a += item.props', { items: records });
      if (edges.length) {
        await tx.run(
          'UNWIND $items AS item MATCH (a:Artifact {id:item.from}), (b:Artifact {id:item.to}) MERGE (a)-[r:RELATED {key:item.id}]->(b) SET r.sourceId=item.sourceId, r.kind=item.kind, r.label=item.label, r.repoId=$repoId',
          { items: edges, repoId }
        );
      }
      const counts = await tx.run(
        'MATCH (a:Artifact {repoId:$repoId}) OPTIONAL MATCH (a)-[r:RELATED {repoId:$repoId}]->() RETURN count(DISTINCT a) AS nodes, count(DISTINCT r) AS edges',
        { repoId }
      );
      const storedNodes = counts.records[0].get('nodes').toNumber();
      const storedEdges = counts.records[0].get('edges').toNumber();
      if (storedNodes < records.length || storedEdges < edges.length) {
        throw new Error(`Read-back mismatch: expected ${records.length} records and ${edges.length} relationships; found ${storedNodes} and ${storedEdges}.`);
      }
      return { persisted: true, storedRecords: storedNodes, relationships: storedEdges };
    });
  } finally {
    await session.close();
  }
}
async function readGraph(fullName) {
  const driver = await getDriver();
  const repoId = `repository:${fullName}`;
  const session = driver.session();
  try {
    const nr = await session.run('MATCH (a:Artifact {repoId:$repoId}) RETURN a ORDER BY a.kind, a.label LIMIT 450', { repoId });
    const er = await session.run('MATCH (a:Artifact {repoId:$repoId})-[r:RELATED {repoId:$repoId}]->(b:Artifact {repoId:$repoId}) RETURN a.id AS from,b.id AS to,r.sourceId AS id,r.kind AS kind,r.label AS label LIMIT 900', { repoId });
    const stored = nr.records.map(r => r.get('a').properties);
    const nodes = stored.filter(p => p.kind !== 'evidence').map(p => ({
      id: p.sourceId || p.id, kind: p.kind, label: p.label, subtitle: p.subtitle,
      url: p.url || null, date: p.date || null, author: p.author || null,
      body: p.body || null, preview: p.preview || null, metadata: parseJSON(p.metadata, {}),
      confidence: p.confidence ?? null
    }));
    const evidence = stored.filter(p => p.kind === 'evidence').map(p => ({
      id: p.evidenceId, kind: p.evidenceKind, title: p.title, body: p.body,
      url: p.url || null, date: p.date || null, author: p.author || null, tags: Array.isArray(p.tags) ? p.tags : parseJSON(p.tags, [])
    }));
    const edges = er.records.map(r => ({ id: r.get('id'), from: stripRepoPrefix(r.get('from'), repoId), to: stripRepoPrefix(r.get('to'), repoId), kind: r.get('kind'), label: r.get('label') }));
    return { nodes, edges, evidence };
  } finally {
    await session.close();
  }
}
async function findEvidenceSeeds(fullName, searchTerms, limit = 3) {
  const terms = [...new Set((searchTerms || []).filter(term => typeof term === 'string' && term.length >= 2))].slice(0, 12);
  if (!terms.length) return [];
  const driver = await getDriver();
  const repoId = `repository:${fullName}`;
  const session = driver.session();
  try {
    const result = await session.run(
      `MATCH (e:Artifact {repoId:$repoId, kind:'evidence'})
       WITH e, [term IN $terms WHERE toLower(coalesce(e.title,'') + ' ' + coalesce(e.body,'') + ' ' + coalesce(e.searchText,'')) CONTAINS term] AS matched
       WHERE size(matched) >= CASE WHEN size($terms) >= 3 THEN 2 ELSE 1 END
       RETURN e.evidenceId AS id, size(matched) AS score
       ORDER BY score DESC, e.date DESC
       LIMIT toInteger($limit)`,
      { repoId, terms, limit: Math.max(1, Math.min(5, Number(limit) || 3)) }
    );
    return result.records.map(record => record.get('id')).filter(Boolean);
  } finally {
    await session.close();
  }
}
function parseJSON(str, fallback) {
  if (typeof str !== 'string') return str || fallback;
  try { return JSON.parse(str); } catch { return fallback; }
}
function stripRepoPrefix(value, repoId) {
  const prefix = `${repoId}::`;
  return String(value || '').startsWith(prefix) ? String(value).slice(prefix.length) : value;
}
async function listPersistedRepositories() {
  if (!isConfigured()) return [];
  try {
    const driver = await getDriver();
    const session = driver.session();
    try {
      const result = await session.run(
        "MATCH (a:Artifact {kind:'repository'}) RETURN a.label AS name, a.subtitle AS description, a.url AS url LIMIT 25"
      );
      return result.records.map(r => ({
        fullName: r.get('name'),
        description: r.get('description') || '',
        url: r.get('url') || '',
        storageMode: 'neo4j'
      }));
    } finally {
      await session.close();
    }
  } catch {
    return [];
  }
}
async function closeNeo4j() { if (driverPromise) { try { const d=await driverPromise; await d.close(); } catch {} driverPromise=null; } }
module.exports = { isConfigured,validateConfiguration,getNeo4jConfig,checkNeo4j,persistGraph,readGraph,findEvidenceSeeds,listPersistedRepositories,closeNeo4j,safeError };
