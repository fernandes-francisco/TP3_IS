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
      tipos: [String!]!
      rankings: [String!]!
    }

    type Ativo {
      Ticker: String
      Nome: String
      MarketCap: String
      ChangePercent: String
      PreviousClose: String
      Open: String
      DaysRange: String
      Week52Range: String
      PERatio: String
      EPS: String
      Beta: String
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
        const tipos = [];
        const rankings = [];
        return { tickers, tipos, rankings };
      } catch (e) {
        console.error('[FILTERS ERROR]', e.message);
        return { tickers: [], tipos: [], rankings: [] };
      }
    },
    search: async ({ q, limit, offset }) => {
      const { getXmlData } = require('../clients/xmlServiceClient');
      const { parseQuery, filterAtivos } = require('../utils/queryParser');
      try {
        const ativos = await getXmlData();
        const filters = parseQuery(q || '');
        const resultados = q ? filterAtivos(ativos, filters) : ativos;
        const mapped = resultados.map(a => ({
          Ticker: a.Ticker,
          Nome: a.Nome,
          MarketCap: a.MarketCap,
          ChangePercent: a.ChangePercent,
          PreviousClose: a.PreviousClose,
          Open: a.Open,
          DaysRange: a.DaysRange,
          Week52Range: a.Week52Range,
          PERatio: a.PERatio,
          EPS: a.EPS,
          Beta: a.Beta,
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
          MarketCap: a.MarketCap,
          ChangePercent: a.ChangePercent,
          PreviousClose: a.PreviousClose,
          Open: a.Open,
          DaysRange: a.DaysRange,
          Week52Range: a.Week52Range,
          PERatio: a.PERatio,
          EPS: a.EPS,
          Beta: a.Beta,
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

  // Endpoint principal de query
  app.get('/api/query', async (req, res) => {
    try {
      const queryString = req.query.q || '';
      console.log(`[HTTP] Query recebida: "${queryString}"`);

      // Conectar ao XML Service e filtrar
      const { getXmlData } = require('../clients/xmlServiceClient');
      const { parseQuery, filterAtivos } = require('../utils/queryParser');

      const ativos = await getXmlData();
      console.log(`[HTTP SUCCESS] ${ativos.length} ativos carregados`);

      // Parsear e filtrar
      const filters = parseQuery(queryString);
      const resultados = queryString ? filterAtivos(ativos, filters) : ativos;

      console.log(`[HTTP] ${resultados.length} resultado(s) retornados`);

      res.json({
        success: true,
        query: queryString,
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

  // Endpoint para query em POST
  app.post('/api/query', async (req, res) => {
    try {
      const queryString = req.body.query || '';
      console.log(`[HTTP POST] Query recebida: "${queryString}"`);

      const { getXmlData } = require('../clients/xmlServiceClient');
      const { parseQuery, filterAtivos } = require('../utils/queryParser');

      let ativos = [];
      try {
        ativos = await getXmlData();
        console.log(`[HTTP POST SUCCESS] ${ativos.length} ativos carregados`);
      } catch (error) {
        console.error('[HTTP POST WARNING] XML Service indisponivel');
        ativos = [
          { Ticker: 'NVDA', Tipo: 'Tecnologia', Ranking: '1', NomeCompleto: 'NVIDIA Corporation' },
          { Ticker: 'AAPL', Tipo: 'Tecnologia', Ranking: '2', NomeCompleto: 'Apple Inc' },
          { Ticker: 'MSFT', Tipo: 'Tecnologia', Ranking: '3', NomeCompleto: 'Microsoft Corporation' }
        ];
      }

      const filters = parseQuery(queryString);
      const resultados = queryString ? filterAtivos(ativos, filters) : ativos;

      console.log(`[HTTP POST] ${resultados.length} resultado(s) retornados`);

      res.json({
        success: true,
        query: queryString,
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

      let ativos = [];
      try {
        ativos = await getXmlData();
      } catch (error) {
        // Mock data como fallback
        ativos = [
          { Ticker: 'NVDA', Tipo: 'Tecnologia', Ranking: '1', NomeCompleto: 'NVIDIA Corporation' },
          { Ticker: 'AAPL', Tipo: 'Tecnologia', Ranking: '2', NomeCompleto: 'Apple Inc' },
          { Ticker: 'MSFT', Tipo: 'Tecnologia', Ranking: '3', NomeCompleto: 'Microsoft Corporation' },
          { Ticker: 'JPM', Tipo: 'Financeiro', Ranking: '1', NomeCompleto: 'JPMorgan Chase & Co' },
          { Ticker: 'WMT', Tipo: 'Bens de Consumo', Ranking: '2', NomeCompleto: 'Walmart Inc' }
        ];
      }

      // Extrair valores únicos para cada campo
      const tickers = [...new Set(ativos.map(a => a.Ticker).filter(v => v))].sort();
      const tipos = [...new Set(ativos.map(a => a.Tipo).filter(v => v))].sort();
      const rankings = [...new Set(ativos.map(a => a.Ranking).filter(v => v))].sort((a, b) => {
        const numA = parseInt(a) || 0;
        const numB = parseInt(b) || 0;
        return numA - numB;
      });

      res.json({
        tickers: tickers,
        tipos: tipos,
        rankings: rankings,
        availableFilters: [
          { name: 'Ticker', type: 'string', example: "symbol='NVDA'" },
          { name: 'Tipo', type: 'string', example: "tipo='Tecnologia'" },
          { name: 'Ranking', type: 'number', example: "ranking='1'" },
          { name: 'NomeCompleto', type: 'string', example: "name='NVIDIA Corporation'" }
        ],
        queryFormat: "Use formato: Campo='valor'",
        examples: [
          { description: 'Retornar todos', query: '' },
          { description: 'Filtrar por Ticker', query: "symbol='NVDA'" },
          { description: 'Filtrar por Tipo', query: "tipo='Tecnologia'" }
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
