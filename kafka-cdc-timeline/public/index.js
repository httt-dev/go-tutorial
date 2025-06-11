const oracleTimeline = document.getElementById('oracle-timeline');
const postgresTimeline = document.getElementById('postgres-timeline');

// Lấy update_user_id từ query parameter
// const urlParams = new URLSearchParams(window.location.search);
// const userId = urlParams.get('update_user_id') || 'default';
const userId = (window.location.pathname.split('/')[1] || 'default').toLowerCase();

// Khởi tạo WebSocket với update_user_id
// const ws = new WebSocket(`ws://192.168.1.199:6869?update_user_id=${encodeURIComponent(userId)}`);
const ws = new WebSocket(`ws://192.168.1.199:6868/${encodeURIComponent(userId)}`);

const MAX_EVENTS = 100;

function limitTimeline(timelineDiv) {
  while (timelineDiv.childNodes.length > MAX_EVENTS) {
    timelineDiv.removeChild(timelineDiv.lastChild);
  }
}

function clearTimeline(type) {
  if (type === 'oracle') {
    oracleTimeline.innerHTML = '';
  } else if (type === 'postgres') {
    postgresTimeline.innerHTML = '';
  }
}

// Hàm hỗ trợ hiển thị giá trị
function formatValue(value) {
  if (value === null || value === undefined) {
    //return '&lt;&lt;NULL&gt;&gt;'; // dùng HTML entity cho dấu "<" và ">"
    return '<span class="null-value">&lt;&lt;NULL&gt;&gt;</span>';
  }
  if (value === '') {
    return '<span class="empty-value">&lt;&lt;EMPTY&gt;&gt;</span>';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}


// Hàm tạo bảng từ dữ liệu before và after
function createTable(before, after) {
  const headers = new Set();
  if (before) Object.keys(before).forEach(key => headers.add(key));
  if (after) Object.keys(after).forEach(key => headers.add(key));
  const headerArray = Array.from(headers);

  let tableHTML = '<table><thead><tr><th>Field</th><th>Before</th><th>After</th></tr></thead><tbody>';

  headerArray.forEach(header => {
    const beforeValueRaw = before ? before[header] : undefined;
    const afterValueRaw = after ? after[header] : undefined;

    const beforeDisplay = formatValue(beforeValueRaw);
    const afterDisplay = formatValue(afterValueRaw);

    const isDifferent = beforeDisplay !== afterDisplay;

    tableHTML += '<tr>';
    tableHTML += `<td>${header}</td>`;
    tableHTML += `<td${isDifferent ? ' class="highlight"' : ''}>${beforeDisplay}</td>`;
    tableHTML += `<td${isDifferent ? ' class="highlight"' : ''}>${afterDisplay}</td>`;
    tableHTML += '</tr>';
  });

  tableHTML += '</tbody></table>';
  return tableHTML;
}

// Khi nhận được message từ WebSocket
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  const { before, after, source, op } = data;

  const eventDiv = document.createElement('div');
  eventDiv.className = 'event';

  const operation = op === 'c' ? 'INSERT' : op === 'u' ? 'UPDATE' : 'DELETE';
  const tableName = source?.table || 'Unknown Table';

  const color = op === 'c' ? '#513eff' : op === 'u' ? '#6ccf11' : '#ff3e3e';

  // Tạo tiêu đề với màu
  eventDiv.innerHTML = `<h3 style="color: ${color}">${operation} on ${tableName} at ${new Date().toLocaleString('ja-JP', { hour12: false })}</h3>`;

  eventDiv.innerHTML += createTable(before, after);

  if (source?.connector === 'oracle') {
    console.log('Adding to Oracle timeline:', data);
    oracleTimeline.insertBefore(eventDiv, oracleTimeline.firstChild);
    limitTimeline(oracleTimeline);
  } else if (source?.connector === 'postgresql') {
    console.log('Adding to PostgreSQL timeline:', data);
    postgresTimeline.insertBefore(eventDiv, postgresTimeline.firstChild);
    limitTimeline(postgresTimeline);
  } else {
    console.warn('Unknown connector:', source?.connector);
  }
};

// Xử lý kết nối WebSocket
ws.onopen = () => console.log(`Connected to WebSocket server with update_user_id: ${userId}`);
ws.onerror = (error) => console.error('WebSocket error:', error);
ws.onclose = () => console.log('WebSocket connection closed');

