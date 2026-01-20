function parseQueryToXPath(queryString) {
  if (!queryString || queryString.trim() === '') {
    return '//Asset';
  }

  const trimmed = queryString.trim();

  // Se o utilizador já enviou XPath direto, devolver tal como está
  if (trimmed.startsWith('//') || trimmed.startsWith('/') || trimmed.startsWith('count(')) {
    return trimmed;
  }

  const normalized = queryString.trim();
  const conditions = [];

  // Formato com aspas: Campo="valor" ou Campo='valor'
  const quotedRegex = /(\w+)\s*=\s*['"]([^'"]+)['"]/g;
  let match;
  while ((match = quotedRegex.exec(normalized)) !== null) {
    const key = match[1].toLowerCase();
    const value = match[2];
    const xpathCondition = buildXPathCondition(key, value);
    if (xpathCondition) conditions.push(xpathCondition);
  }

  // Formato sem aspas: symbol=AAPL, sector=technology
  const parts = normalized.split(/[,&\s]+/).filter(Boolean);
  let hasStructuredFilters = false;
  
  parts.forEach((part) => {
    const [rawKey, rawValue] = part.split('=');
    if (!rawKey || rawValue === undefined) return;

    hasStructuredFilters = true;
    const key = rawKey.trim().toLowerCase();
    const value = rawValue.trim();

    if (!value) return;

    const xpathCondition = buildXPathCondition(key, value);
    if (xpathCondition && !conditions.includes(xpathCondition)) {
      conditions.push(xpathCondition);
    }
  });

  // Se não há filtros estruturados, buscar em múltiplos campos
  if (!hasStructuredFilters && conditions.length === 0 && normalized) {
    // Busca livre: procurar em ticker, nome e sector
    const freeSearch = normalized.replace(/'/g, "\\'");
    const freeLower = freeSearch.toLowerCase();
    return `//Asset[@Ticker='${freeSearch}' or contains(translate(Identification/Name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '${freeLower}') or contains(translate(Identification/Sector, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '${freeLower}')]`;
  }

  // Construir XPath final
  if (conditions.length === 0) {
    return '//Asset';
  } else if (conditions.length === 1) {
    return `//Asset[${conditions[0]}]`;
  } else {
    return `//Asset[${conditions.join(' and ')}]`;
  }
}

function buildXPathCondition(key, value) {
  // Mapear aliases para campos XPath
  const keyMap = {
    symbol: 'Ticker',
    ticker: 'Ticker',
    nome: 'Name',
    name: 'Name',
    sector: 'Sector',
    setor: 'Sector'
  };

  const mappedKey = keyMap[key] || key;
  const escapedValue = value.replace(/'/g, "\\'");

  switch (mappedKey) {
    case 'Ticker':
      return `@Ticker='${escapedValue}'`;
    case 'Name':
      return `contains(translate(Identification/Name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '${escapedValue.toLowerCase()}')`;
    case 'Sector':
      return `Identification/Sector='${escapedValue}'`;
    default:
      return null;
  }
}

module.exports = { parseQueryToXPath };

