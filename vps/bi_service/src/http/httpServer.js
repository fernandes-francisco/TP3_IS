const express = require('express');
const cors = require('cors');
const { buildSchema } = require('graphql');
const { graphqlHTTP } = require('express-graphql');

/**
 * Cria e configura o servidor HTTP Express
 * Fornece endpoint para chamar servico gRPC sem precisar de cliente gRPC
 */
function createHttpServer(queryService) {
  const app = express();

  // Middleware
  app.use(cors());
  app.use(express.json());

  // Health check
  app.get('/health', (req, res) => {
    res.json({ status: 'OK', service: 'BI Service', timestamp: new Date().toISOString() });
  });

  // Info endpoint
  app.get('/info', (req, res) => {
    res.json({
      name: 'BI Service',
      version: '1.0.0',
      description: 'Business Intelligence Service para analise de ativos financeiros',
      protocols: ['gRPC', 'HTTP'],
      endpoints: {
        http: 'http://localhost:' + (process.env.HTTP_PORT || 8000),
        grpc: 'localhost:' + (process.env.PORT || 5001),
        graphql: '/graphql'
      }
    });
  });

  // GraphQL schema e resolvers
  const schema = buildSchema(`
    type Health {
      status: String!
      timestamp: String!
    }

    type Filters {
      tickers: [String!]!
      sectors: [String!]!
    }

    type Ativo {
      Ticker: String
      Nome: String
      Sector: String
      PriceSMA: String
      AverageVolume: String
      Prices: [String!]
      Volumes: [String!]
      raw: String
    }

    type PaginatedAtivos {
      total: Int!
      limit: Int!
      offset: Int!
      count: Int!
      data: [Ativo!]!
    }

    type Query {
      health: Health!
      filters: Filters!
      search(q: String, limit: Int, offset: Int): PaginatedAtivos!
      ativos(ticker: String, name: String, limit: Int, offset: Int): PaginatedAtivos!
    }
  `);

  // Helper para paginação
  const paginate = (items, limit = 50, offset = 0) => {
    const actualLimit = Math.min(Math.max(limit || 50, 1), 1000); // min 1, max 1000
    const actualOffset = Math.max(offset || 0, 0);
    const total = items.length;
    const paged = items.slice(actualOffset, actualOffset + actualLimit);
    
    return {
      total,
      limit: actualLimit,
      offset: actualOffset,
      count: paged.length,
      data: paged
    };
  };

  const rootResolvers = {
    health: async () => ({ status: 'OK', timestamp: new Date().toISOString() }),
    filters: async () => {
      const { getXmlData } = require('../clients/xmlServiceClient');
      try {
        const ativos = await getXmlData();
        const tickers = [...new Set(ativos.map(a => a.Ticker).filter(Boolean))].sort();
        const sectors = [...new Set(ativos.map(a => a.Sector).filter(Boolean))].sort();
        return { tickers, sectors };
      } catch (e) {
        console.error('[FILTERS ERROR]', e.message);
        return { tickers: [], sectors: [] };
      }
    },
    search: async ({ q, limit, offset }) => {
      const { getXmlData } = require('../clients/xmlServiceClient');
      const { parseQueryToXPath } = require('../utils/queryParser');
      try {
        const xpathQuery = parseQueryToXPath(q || '');
        const ativos = await getXmlData(xpathQuery);
        const mapped = ativos.map(a => ({
          Ticker: a.Ticker,
          Nome: a.Nome,
          Sector: a.Sector,
          PriceSMA: a.PriceSMA,
          AverageVolume: a.AverageVolume,
          Prices: a.Prices,
          Volumes: a.Volumes,
          raw: JSON.stringify(a)
        }));
        return paginate(mapped, limit, offset);
      } catch (e) {
        console.error('[SEARCH ERROR]', e.message);
        return { total: 0, limit: limit || 50, offset: offset || 0, count: 0, data: [] };
      }
    },
    ativos: async ({ ticker, name, limit, offset }) => {
      const { getXmlData } = require('../clients/xmlServiceClient');
      try {
        const ativos = await getXmlData();
        const matches = (a) => (
          (!ticker || String(a.Ticker).toLowerCase() === String(ticker).toLowerCase()) &&
          (!name || String(a.Nome || '').toLowerCase().includes(String(name).toLowerCase()))
        );
        const resultados = ativos.filter(matches);
        const mapped = resultados.map(a => ({
          Ticker: a.Ticker,
          Nome: a.Nome,
          Sector: a.Sector,
          PriceSMA: a.PriceSMA,
          AverageVolume: a.AverageVolume,
          Prices: a.Prices,
          Volumes: a.Volumes,
          raw: JSON.stringify(a)
        }));
        return paginate(mapped, limit, offset);
      } catch (e) {
        console.error('[ATIVOS ERROR]', e.message);
        return { total: 0, limit: limit || 50, offset: offset || 0, count: 0, data: [] };
      }
    }
  };

  app.use('/graphql', graphqlHTTP({
    schema,
    rootValue: rootResolvers,
    graphiql: true
  }));

  // Endpoint XPath para queries específicas (preset ou custom)
  app.get('/api/xpath', async (req, res) => {
    try {
      // Aceita 'type' para presets ou 'query' para XPath custom
      const queryType = req.query.type || req.query.query || 'allAssets';
      console.log(`[XPATH] Query recebida: "${queryType}"`);

      const { getXmlData } = require('../clients/xmlServiceClient');

      const resultados = await getXmlData(queryType);
      console.log(`[XPATH SUCCESS] ${resultados.length} resultado(s)`);

      res.json({
        success: true,
        query: queryType,
        count: resultados.length,
        data: resultados,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('[XPATH ERROR]', error.message);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  // Endpoint principal de query (converte sintaxe user-friendly para XPath)
  app.get('/api/query', async (req, res) => {
    try {
      const queryString = req.query.q || '';
      console.log(`[HTTP] Query recebida: "${queryString}"`);

      const { getXmlData } = require('../clients/xmlServiceClient');
      const { parseQueryToXPath } = require('../utils/queryParser');

      // Converter para XPath
      const xpathQuery = parseQueryToXPath(queryString);
      console.log(`[HTTP] XPath gerado: ${xpathQuery}`);

      const resultados = await getXmlData(xpathQuery);
      console.log(`[HTTP SUCCESS] ${resultados.length} resultado(s) retornados`);

      res.json({
        success: true,
        query: queryString,
        xpath: xpathQuery,
        count: resultados.length,
        data: resultados,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('[HTTP ERROR]', error.message);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  // Endpoint para query em POST (converte sintaxe user-friendly para XPath)
  app.post('/api/query', async (req, res) => {
    try {
      const queryString = req.body.query || '';
      console.log(`[HTTP POST] Query recebida: "${queryString}"`);

      const { getXmlData } = require('../clients/xmlServiceClient');
      const { parseQueryToXPath } = require('../utils/queryParser');

      // Converter para XPath
      const xpathQuery = parseQueryToXPath(queryString);
      console.log(`[HTTP POST] XPath gerado: ${xpathQuery}`);

      const resultados = await getXmlData(xpathQuery);
      console.log(`[HTTP POST SUCCESS] ${resultados.length} resultado(s) retornados`);

      res.json({
        success: true,
        query: queryString,
        xpath: xpathQuery,
        count: resultados.length,
        data: resultados,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('[HTTP POST ERROR]', error.message);
      res.status(500).json({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  // Rota para filtros disponiveis
  app.get('/api/filters', async (req, res) => {
    try {
      const { getXmlData } = require('../clients/xmlServiceClient');

      const ativos = await getXmlData();

      // Extrair valores únicos para cada campo
      const tickers = [...new Set(ativos.map(a => a.Ticker).filter(v => v))].sort();
      const sectors = [...new Set(ativos.map(a => a.Sector).filter(v => v))].sort();

      res.json({
        tickers: tickers,
        sectors: sectors,
        availableFilters: [
          { name: 'Ticker', type: 'string', example: "symbol='AAPL'" },
          { name: 'Sector', type: 'string', example: "sector='Technology'" },
          { name: 'Nome', type: 'string', example: "name='Apple Inc'" }
        ],
        queryFormat: "Use formato: Campo='valor'",
        examples: [
          { description: 'Retornar todos', query: '' },
          { description: 'Filtrar por Ticker', query: "symbol='AAPL'" },
          { description: 'Filtrar por Sector', query: "sector='Technology'" }
        ]
      });
    } catch (error) {
      console.error('[FILTERS ERROR]', error.message);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  });

  // 404 handler
  app.use((req, res) => {
    res.status(404).json({
      error: 'Endpoint nao encontrado',
      path: req.path,
      method: req.method
    });
  });

  return app;
}

module.exports = { createHttpServer };
