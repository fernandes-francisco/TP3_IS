var API_BASE_URL = 'http://localhost:8000';
var currentResultsData = [];
var availableFilters = {};
var queryHistory = [];

// Paginação
var currentQuery = '';
var currentPage = 1;
var itemsPerPage = 20;
var totalItems = 0;
var paginationInfo = { total: 0, limit: 20, offset: 0, count: 0 };

// Ordenação
var currentSortOrder = '';

function updateStatus() {
  var dot = document.getElementById('service-status-dot');
  var text = document.getElementById('service-status-text');
  var time = document.getElementById('service-status-time');

  // GraphQL health query
  fetch(API_BASE_URL + '/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'query { health { status timestamp } }' })
    })
    .then(function(response) { return response.json(); })
    .then(function(graph) {
      var data = graph.data && graph.data.health ? graph.data.health : null;
      dot.className = 'status-badge online';
      text.textContent = 'Online';
      var now = new Date();
      var hours = now.getHours();
      var minutes = now.getMinutes();
      var ampm = hours >= 12 ? 'PM' : 'AM';
      hours = hours % 12;
      hours = hours ? hours : 12;
      minutes = minutes < 10 ? '0' + minutes : minutes;
      time.textContent = 'Atualizado às ' + hours + ':' + minutes + ' ' + ampm;
    })
    .catch(function() {
      dot.className = 'status-badge offline';
      text.textContent = 'Offline';
      time.textContent = '';
    });
}

function applyGuidedFilters() {
  var ticker = document.getElementById('ticker-select').value;

  var query = '';
  if (ticker) query = 'symbol=' + ticker;

  executeQuery(query);
}

function applySorting() {
  var sortOrder = document.getElementById('sort-order').value;
  currentSortOrder = sortOrder;
  
  if (currentResultsData && currentResultsData.length > 0) {
    sortAndDisplayResults();
  }
}

function sortAndDisplayResults() {
  var sorted = currentResultsData.slice();
  
  if (currentSortOrder === 'asc') {
    sorted.sort(function(a, b) {
      var priceA = parseFloat(a.MarketCap) || 0;
      var priceB = parseFloat(b.MarketCap) || 0;
      return priceA - priceB;
    });
  } else if (currentSortOrder === 'desc') {
    sorted.sort(function(a, b) {
      var priceA = parseFloat(a.MarketCap) || 0;
      var priceB = parseFloat(b.MarketCap) || 0;
      return priceB - priceA;
    });
  }
  
  var resultsBox = document.getElementById('results-container');
  var count = document.getElementById('results-count');
  displayResults(sorted, count, resultsBox);
}

function executeManualQuery() {
  var input = document.getElementById('query-input');
  var query = input.value.trim();
  executeQuery(query);
}

function executeQuery(query) {
  query = query || '';
  var resultsBox = document.getElementById('results-container');
  var count = document.getElementById('results-count');
  var errorBox = document.getElementById('error-box');

  if (!resultsBox || !count || !errorBox) {
    console.error('Elementos do DOM não encontrados em executeQuery');
    return;
  }

  errorBox.style.display = 'none';
  resultsBox.innerHTML = '<p class="placeholder"><span class="loader"></span> A processar...</p>';

  // Guardar query e resetar paginação
  currentQuery = query;
  currentPage = 1;
  paginationInfo = { total: 0, limit: itemsPerPage, offset: 0, count: 0 };

  // Adicionar ao histórico se não estiver vazio
  if (query && query.trim()) {
    addToHistory(query);
  }

  // GraphQL search query com paginação
  var offset = (currentPage - 1) * itemsPerPage;
  fetch(API_BASE_URL + '/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 
        query: 'query($q:String, $limit:Int, $offset:Int){ search(q:$q, limit:$limit, offset:$offset){ total limit offset count data { Ticker Nome MarketCap ChangePercent PreviousClose Open DaysRange Week52Range PERatio EPS Beta raw } } }', 
        variables: { q: query, limit: itemsPerPage, offset: offset } 
      })
    })
    .then(function(response) { return response.json(); })
    .then(function(graph) {
      // Verificar se houve erros na resposta GraphQL PRIMEIRO
      if (graph.errors && graph.errors.length > 0) {
        showError('Erro no servidor: ' + graph.errors[0].message);
        return;
      }
      
      var result = (graph.data && graph.data.search) ? graph.data.search : null;
      if (!result) {
        showError('Erro ao obter dados.');
        return;
      }
      
      // Verificar se não houve resultados
      if (!result.data || result.data.length === 0) {
        var errorBox = document.getElementById('error-box');
        var resultsBox = document.getElementById('results-container');
        errorBox.style.display = 'block';
        errorBox.textContent = 'Nenhum resultado encontrado. Verifique os critérios de pesquisa (ticker ou nome).';
        resultsBox.innerHTML = '<p class="placeholder">Sem dados.</p>';
        document.getElementById('results-count').textContent = '0';
        return;
      }
      
      paginationInfo = result;
      currentResultsData = result.data || [];
      totalItems = result.total || 0;
      document.getElementById('error-box').style.display = 'none';
      
      // Aplicar ordenação se houver
      if (currentSortOrder) {
        sortAndDisplayResults();
      } else {
        displayResults(currentResultsData, count, resultsBox);
      }
    })
    .catch(function(error) {
      showError('Erro ao executar query: ' + error.message);
    });
}

function addToHistory(query) {
  // Evitar duplicatas consecutivas
  if (queryHistory.length > 0 && queryHistory[queryHistory.length - 1] === query) {
    return;
  }
  queryHistory.unshift(query);
  updateHistoryDisplay();
}

function updateHistoryDisplay() {
  var container = document.getElementById('history-container');
  var count = document.getElementById('history-count');
  
  if (queryHistory.length === 0) {
    container.innerHTML = '<p class="placeholder">Nenhuma pesquisa realizada ainda.</p>';
    count.textContent = '0';
    return;
  }

  count.textContent = String(queryHistory.length);

  var html = '';
  queryHistory.forEach(function(query, index) {
    html += '<div class="history-item">' +
              '<div class="history-query">' + query + '</div>' +
              '<button class="history-rerun" onclick="executeQuery(\'' + query.replace(/'/g, "\\'") + '\')">Executar Novamente</button>' +
            '</div>';
  });

  container.innerHTML = html;
}

function rerunQuery(query) {
  document.getElementById('query-input').value = query;
  executeQuery(query);
}

function showError(message) {
  var errorBox = document.getElementById('error-box');
  var resultsBox = document.getElementById('results-container');
  var countBox = document.getElementById('results-count');
  
  if (!errorBox || !resultsBox || !countBox) {
    console.error('Elementos do DOM não encontrados');
    return;
  }
  
  errorBox.textContent = message;
  errorBox.style.display = 'block';
  resultsBox.innerHTML = '<p class="placeholder">Sem dados.</p>';
  countBox.textContent = '0';
}

function displayResults(items, countEl, container) {
  var errorBox = document.getElementById('error-box');
  
  if (!items || items.length === 0) {
    errorBox.style.display = 'block';
    errorBox.textContent = 'Nenhum resultado encontrado. Verifique os critérios de pesquisa (ticker ou nome).';
    container.innerHTML = '<p class="placeholder">Sem dados.</p>';
    countEl.textContent = '0';
    return;
  }

  // Limpar mensagem de erro se houver resultados
  errorBox.style.display = 'none';
  countEl.textContent = String(paginationInfo.count) + ' de ' + String(paginationInfo.total);

  // Colunas a exibir: Ticker, Nome, MarketCap, ChangePercent
  var displayColumns = ['Ticker', 'Nome', 'MarketCap', 'ChangePercent'];
  
  var html = '<div class="table-wrapper"><table class="result-table">';
  html += '<thead><tr>';
  displayColumns.forEach(function(col) {
    html += '<th>' + formatKeyName(col) + '</th>';
  });
  html += '</tr></thead><tbody>';
  
  items.forEach(function(row, index){
    html += '<tr class="clickable-row" data-index="' + index + '" style="cursor: pointer;">';
    displayColumns.forEach(function(col) {
      var value = row[col] || '-';
      html += '<td>' + value + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table></div>';

  // Adicionar paginação
  var currentPageNum = currentPage;
  var totalPages = Math.ceil(paginationInfo.total / itemsPerPage);
  
  html += '<div class="pagination-controls" style="text-align: center; margin-top: 20px; padding: 10px 0;">';
  
  if (currentPageNum > 1) {
    html += '<button onclick="previousPage()" class="primary" style="margin: 0 5px;">← Anterior</button>';
  }
  if (currentPageNum < totalPages) {
    html += '<button onclick="nextPage()" class="primary" style="margin: 0 5px;">Próxima →</button>';
  }
  
  html += '<div style="margin-top: 15px; color: #64748b; font-size: 14px;">Página ' + currentPageNum + ' de ' + totalPages + '</div>';
  
  html += '</div>';

  container.innerHTML = html;

  // Adicionar listeners aos cliques nas linhas
  var rows = container.querySelectorAll('.clickable-row');
  rows.forEach(function(row) {
    row.addEventListener('click', function() {
      var index = parseInt(this.getAttribute('data-index'), 10);
      showDetails(items[index]);
    });
  });
}

function nextPage() {
  var totalPages = Math.ceil(paginationInfo.total / itemsPerPage);
  if (currentPage < totalPages) {
    currentPage++;
    executeQuery(currentQuery);
  }
}

function previousPage() {
  if (currentPage > 1) {
    currentPage--;
    executeQuery(currentQuery);
  }
}

function showDetails(item) {
  var modal = document.getElementById('details-modal');
  var title = document.getElementById('details-title');
  var body = document.getElementById('details-body');

  title.textContent = item.Ticker || item.Symbol || 'Detalhes do Ativo';

  var html = '<div class="details-grid">';
  for (var key in item) {
    if (item.hasOwnProperty(key)) {
      var value = item[key];
      var displayKey = formatKeyName(key);
      var displayValue = formatValue(value);
      html += '<div class="detail-item">';
      html += '<span class="detail-label">' + displayKey + '</span>';
      html += '<span class="detail-value">' + displayValue + '</span>';
      html += '</div>';
    }
  }
  html += '</div>';

  body.innerHTML = html;
  modal.style.display = 'flex';
}

function closeModal() {
  var modal = document.getElementById('details-modal');
  modal.style.display = 'none';
}

function formatKeyName(key) {
  // Tratamento especial para NomeCompleto
  if (key === 'NomeCompleto') return 'Nome Completo';
  
  return key
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, function(str) { return str.toUpperCase(); })
    .trim();
}

function formatValue(value) {
  if (value === null || value === undefined) return '-';
  if (typeof value === 'number') return value.toLocaleString('pt-PT');
  if (typeof value === 'object') {
    try { return JSON.stringify(value, null, 2); } catch (e) { return String(value); }
  }
  return String(value);
}



function loadGuidedFilters() {
  // GraphQL filters query
  fetch(API_BASE_URL + '/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: 'query { filters { tickers tipos rankings } }' })
    })
    .then(function(response) { return response.json(); })
    .then(function(graph) {
      var data = graph.data && graph.data.filters ? graph.data.filters : { tickers: [], tipos: [], rankings: [] };
      populateGuidedSelects(data);
    })
    .catch(function(error) {
      console.log('Erro ao carregar filtros guiados:', error);
    });
}

function populateGuidedSelects(data) {
  var tickers = (data && data.tickers) || [];

  populateSelect('ticker-select', tickers);
}

function populateSelect(selectId, options) {
  var select = document.getElementById(selectId);
  if (!select) return;

  while (select.options.length > 1) {
    select.remove(1);
  }

  if (options && options.length > 0) {
    options.forEach(function(opt) {
      var option = document.createElement('option');
      option.value = opt;
      option.textContent = opt;
      select.appendChild(option);
    });
  }
}

document.addEventListener('DOMContentLoaded', function() {
  updateStatus();
  loadGuidedFilters();
  showInitialMessage();
  setInterval(updateStatus, 5000);

  // Fechar modal ao clicar fora
  var modal = document.getElementById('details-modal');
  window.addEventListener('click', function(event) {
    if (event.target === modal) {
      closeModal();
    }
  });
});

function showInitialMessage() {
  var resultsBox = document.getElementById('results-container');
  var count = document.getElementById('results-count');
  var errorBox = document.getElementById('error-box');
  
  errorBox.style.display = 'none';
  resultsBox.innerHTML = '<p class="placeholder">Ainda não foi realizada nenhuma pesquisa.</p>';
  count.textContent = '0';
}

