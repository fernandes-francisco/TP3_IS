const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const xml2js = require('xml2js');

const XML_SERVICE_GRPC_URL = process.env.XML_SERVICE_GRPC_URL || 'localhost:50051';

// Carregar proto
const PROTO_PATH = path.join(__dirname, '../../proto/bi_request.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const xmlQueryService = protoDescriptor.bi_request;

// Cliente gRPC
let grpcClient = null;

function getGrpcClient() {
  if (!grpcClient) {
    grpcClient = new xmlQueryService.XmlQueryService(
      XML_SERVICE_GRPC_URL,
      grpc.credentials.createInsecure()
    );
  }
  return grpcClient;
}

/**
 * Converte recursivamente estrutura XML (xml2js) para objeto JavaScript plano
 * @param {Object} obj - Objeto do xml2js
 * @returns {Object|Array|string} Objeto normalizado
 */
function deepNormalize(obj) {
  if (typeof obj === 'string') return obj;
  if (Array.isArray(obj)) {
    if (obj.length === 1 && typeof obj[0] === 'string') return obj[0]; // valor de texto simples
    return obj.map(item => deepNormalize(item));
  }
  if (typeof obj !== 'object' || obj === null) return obj;

  const result = {};

  // Processar atributos (prefixo $)
  if (obj.$ && Object.keys(obj.$).length > 0) {
    Object.assign(result, obj.$);
  }

  // Processar elementos filhos
  Object.keys(obj).forEach(key => {
    if (key === '$') return; // já processado
    const value = obj[key];
    
    if (Array.isArray(value)) {
      // Se é array com um único elemento simples, desembrulha
      if (value.length === 1 && typeof value[0] === 'string') {
        result[key] = value[0];
      } else if (value.length === 1 && typeof value[0] === 'object') {
        result[key] = deepNormalize(value[0]);
      } else {
        // Array com múltiplos elementos - normaliza cada um
        result[key] = value.map(item => deepNormalize(item));
      }
    } else {
      result[key] = deepNormalize(value);
    }
  });

  return result;
}

/**
 * Normaliza um nó <Asset> para um objeto com campos do crawler Yahoo Finance
 * Estrutura XML esperada do converter:
 * <Asset Ticker="AAPL">
 *   <Identification><Name>...</Name></Identification>
 *   <FundamentalData><MarketCap>...</MarketCap><PERatio>...</PERatio><EPS>...</EPS><Beta>...</Beta></FundamentalData>
 *   <DailyData><Day><ClosingPrice>...</ClosingPrice><Volume>...</Volume></Day>...</DailyData>
 * </Asset>
 */
function normalizeAtivo(ativo) {
  const obj = deepNormalize(ativo);
  const identification = obj.Identification || {};
  const fundamental = obj.FundamentalData || {};
  const dailyData = obj.DailyData && obj.DailyData.Day ? obj.DailyData.Day : [];

  return {
    // Campos principais do crawler
    Ticker: obj.Ticker,
    Nome: identification.Name,
    
    // Fundamental Data
    MarketCap: fundamental.MarketCap,
    ChangePercent: fundamental.ChangePercent,
    PreviousClose: fundamental.PreviousClose,
    Open: fundamental.Open,
    DaysRange: fundamental.DaysRange,
    Week52Range: fundamental.Week52Range,
    PERatio: fundamental.PERatio,
    EPS: fundamental.EPS,
    Beta: fundamental.Beta,

    // Secções aninhadas (para modal de detalhes)
    Identification: identification,
    FundamentalData: fundamental,
    DailyData: Array.isArray(dailyData) ? dailyData : [dailyData],

    // Objeto completo normalizado
    raw: obj
  };
}

async function getXmlData(filters = {}) {
  return new Promise((resolve, reject) => {
    try {
      console.log(`[CONNECT] A Conectar ao XML Service gRPC em ${XML_SERVICE_GRPC_URL}...`);
      
      const client = getGrpcClient();
      
      // XPath query para buscar todos os Assets
      const xpathQuery = '//Asset';
      
      const call = client.GetQueryResult({ QueryString: xpathQuery });
      
      const results = [];
      
      call.on('data', (response) => {
        try {
          // Parse XML fragment retornado
          const xmlFragment = response.Result;
          const parser = new (require('xml2js')).Parser();
          
          parser.parseString(xmlFragment, (err, parsed) => {
            if (!err && parsed && parsed.Asset) {
              const normalized = normalizeAtivo(parsed.Asset);
              results.push(normalized);
            }
          });
        } catch (parseErr) {
          console.error('[PARSE ERROR]', parseErr.message);
        }
      });
      
      call.on('end', () => {
        console.log(`[PARSED] ${results.length} ativos parseados com sucesso via gRPC`);
        resolve(results);
      });
      
      call.on('error', (err) => {
        console.error('[ERROR] Erro ao conectar XML Service gRPC:', err.message);
        reject(err);
      });
      
    } catch (error) {
      console.error('[ERROR] Erro ao conectar XML Service:', error.message);
      reject(error);
    }
  });
}

module.exports = { getXmlData };
