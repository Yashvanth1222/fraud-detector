import { google } from 'googleapis';

let sheetsClient = null;

function getAuth() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  const fixed = raw.replace(/\n/g, '\\n');
  const creds = JSON.parse(fixed);
  return new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
}

function getSheets() {
  if (!sheetsClient) {
    sheetsClient = google.sheets({ version: 'v4', auth: getAuth() });
  }
  return sheetsClient;
}

export async function readSheet(sheetId, range = 'Sheet1') {
  const sheets = getSheets();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range
  });
  const rows = res.data.values || [];
  if (rows.length < 2) return [];

  const headers = rows[0];
  return rows.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = row[i] || ''; });
    return obj;
  });
}

export async function appendRows(sheetId, rows, range = 'Sheet1') {
  if (rows.length === 0) return;
  const sheets = getSheets();
  const headers = Object.keys(rows[0]);

  // Check if sheet has headers already
  let hasHeaders = false;
  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${range}!A1:A1`
    });
    hasHeaders = (existing.data.values || []).length > 0;
  } catch (e) { /* empty sheet */ }

  const values = [];
  if (!hasHeaders) values.push(headers);
  for (const row of rows) {
    values.push(headers.map(h => row[h] ?? ''));
  }

  await sheets.spreadsheets.values.append({
    spreadsheetId: sheetId,
    range,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  });
}
