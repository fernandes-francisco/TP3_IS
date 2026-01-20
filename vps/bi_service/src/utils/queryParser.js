/**
 * Parser simples para queries do formato: "Ticker='NVDA'" ou "Tipo='Tecnologia'"
 * @param {string} queryString - String da query
 * @returns {Object} Objeto com filtros { campo: valor }
 */
function parseQuery(queryString) {
  if (!queryString || queryString.trim() === '') {
    return {};
  }

  const normalized = queryString.trim();
  const filters = {};

  // 1) Suporta formato com aspas: Campo="valor" ou Campo='valor'
  const quotedRegex = /(\w+)\s*=\s*['"]([^'"]+)['"]/g;
  let match;
  while ((match = quotedRegex.exec(normalized)) !== null) {
    filters[match[1]] = match[2];
  }

  // 2) Suporta formato sem aspas e case-insensitive (ex: symbol=NVDA, sector=technology)
  //    separando por , ou & ou espaços
  const parts = normalized.split(/[,&\s]+/).filter(Boolean);
  let hasStructuredFilters = false;
  
  parts.forEach((part) => {
    const [rawKey, rawValue] = part.split('=');
    if (!rawKey || rawValue === undefined) return;

    hasStructuredFilters = true;
    const key = rawKey.trim().toLowerCase();
    const value = rawValue.trim();

    if (!value) return;

    // Mapear aliases para campos do dataset
    const keyMap = {
      // Ticker
      symbol: 'Ticker',
      ticker: 'Ticker',
      
      // Nome
      nome: 'Nome',
      name: 'Nome',
      
      // Market Cap
      marketcap: 'MarketCap',
      cap: 'MarketCap'
    };

    const mappedKey = keyMap[key] || rawKey;
    // Se já havia capturado via regex com aspas, não sobrescreve
    if (filters[mappedKey] === undefined) {
      filters[mappedKey] = value;
    }
  });

  // 3) Se não há filtros estruturados (campo=valor), tratar como busca livre
  if (!hasStructuredFilters && Object.keys(filters).length === 0 && normalized) {
    filters._freeSearch = normalized;
  }

  return filters;
}

/**
 * Filtra array de ativos baseado nos filtros fornecidos
 * @param {Array} ativos - Array de ativos
 * @param {Object} filters - Objeto com filtros { campo: valor }
 * @returns {Array} Ativos filtrados
 */
function filterAtivos(ativos, filters) {
  if (!filters || Object.keys(filters).length === 0) {
    return ativos;
  }

  return ativos.filter((ativo) => {
    for (const [key, value] of Object.entries(filters)) {
      // Suportar ambas as estruturas: campo direto ou aninhado em Identification
      let ativoValue;
      
      if (key === 'Nome') {
        // Pode estar em ativo.Nome ou em ativo.Identification.Name
        ativoValue = ativo[key] || (ativo.Identification && ativo.Identification.Name);
      } else if (key === '_freeSearch') {
        // Busca livre: procura em Ticker e Nome
        const searchTerm = String(value || '').toLowerCase().trim();
        const ticker = String(ativo.Ticker || '').toLowerCase();
        const nome = String(ativo.Nome || '').toLowerCase();
        
        const matches = ticker.includes(searchTerm) || nome.includes(searchTerm);
        
        if (!matches) {
          return false;
        }
        continue;
      } else {
        // Outros campos
        ativoValue = ativo[key];
      }
      
      if (Array.isArray(ativoValue)) {
        ativoValue = ativoValue[0];
      }
      
      const ativoStr = String(ativoValue || '').toLowerCase().trim();
      const filterStr = String(value || '').toLowerCase().trim();

      // Para Ticker: exact match
      // Para Nome: partial match (contains)
      let matches = false;
      if (key === 'Ticker') {
        matches = ativoStr === filterStr;
      } else {
        // Partial match (contains)
        matches = ativoStr.includes(filterStr);
      }

      if (!matches) {
        return false;
      }
    }
    return true;
  });
}

module.exports = { parseQuery, filterAtivos };

