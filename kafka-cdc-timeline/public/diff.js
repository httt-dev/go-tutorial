
let selectedOracleTable = null;
let selectedPostgresTable = null;

document.addEventListener('contextmenu', function (e) {
  const isOracle = e.target.closest('#oracle-timeline .event table');
  const isPostgres = e.target.closest('#postgres-timeline .event table');

  if (isOracle) {
    e.preventDefault();
    if (selectedOracleTable) selectedOracleTable.style.border = '';
    if (selectedOracleTable === isOracle) {
      selectedOracleTable = null;
    } else {
      selectedOracleTable = isOracle;
      isOracle.style.border = '2px dashed orange';
    }
  }

  if (isPostgres) {
    e.preventDefault();
    if (selectedPostgresTable) selectedPostgresTable.style.border = '';
    if (selectedPostgresTable === isPostgres) {
      selectedPostgresTable = null;
    } else {
      selectedPostgresTable = isPostgres;
      isPostgres.style.border = '2px dashed #36cd36';
    }
  }

  // Kiểm tra nếu cả hai bảng được chọn → hiển thị nút Diff
  const diffBtn = document.getElementById('diff-btn');
  if (selectedOracleTable && selectedPostgresTable) {
    diffBtn.style.display = 'block';
  } else {
    diffBtn.style.display = 'none';
  }
});


document.getElementById('diff-btn').addEventListener('click', () => {

  sessionStorage.setItem('oracleData', JSON.stringify({
    before: extractTableData(selectedOracleTable, 'before'),
    after: extractTableData(selectedOracleTable, 'after')
  }));
  sessionStorage.setItem('postgresData', JSON.stringify({
    before: extractTableData(selectedPostgresTable, 'before'),
    after: extractTableData(selectedPostgresTable, 'after')
  }));
  
  
  window.open('diff.html', '_blank');
});


function extractTableData(table, type) {
  const rows = Array.from(table.querySelectorAll('tbody tr'));
  const data = {};
  for (const row of rows) {
    const cells = row.querySelectorAll('td');
    const field = cells[0].textContent.trim();
    const value = (type === 'before') ? cells[1].textContent.trim() : cells[2].textContent.trim();
    data[field] = value;
  }
  // console.log(`Extracted ${type} data:`, data);
  return data;
}
