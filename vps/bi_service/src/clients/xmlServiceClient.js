const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const xml2js = require('xml2js');

const XML_SERVICE_GRPC_URL = process.env.XML_SERVICE_GRPC_URL || 'localhost:50051';

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

// Converte XML (xml2js) para objeto JavaScript plano
function deepNormalize(obj) {
  if (typeof obj === 'string') return obj;
  if (Array.isArray(obj)) {
    if (obj.length === 1 && typeof obj[0] === 'string') return obj[0];
    return obj.map(item => deepNormalize(item));
  }
  if (typeof obj !== 'object' || obj === null) return obj;

  const result = {};

  if (obj.$ && Object.keys(obj.$).length > 0) {
    Object.assign(result, obj.$);
  }

  Object.keys(obj).forEach(key => {
    if (key === '$') return;
    const value = obj[key];
    
    if (Array.isArray(value)) {
      if (value.length === 1 && typeof value[0] === 'string') {
        result[key] = value[0];
      } else if (value.length === 1 && typeof value[0] === 'object') {
        result[key] = deepNormalize(value[0]);
      } else {
        result[key] = value.map(item => deepNormalize(item));
      }
    } else {
      result[key] = deepNormalize(value);
    }
  });

  return result;
}

function normalizeAtivo(ativo) {
  const obj = deepNormalize(ativo);
  const identification = obj.Identification || {};
  const indicators = obj.Indicators || {};
  const dailyData = obj.DailyData && obj.DailyData.Day ? obj.DailyData.Day : [];

  // Extrair primeiros 10 dias de preços e volumes
  const days = Array.isArray(dailyData) ? dailyData : (dailyData ? [dailyData] : []);
  const prices = days.map(d => {
    const normalized = deepNormalize(d);
    return normalized.ClosingPrice;
  }).slice(0, 10);
  
  const volumes = days.map(d => {
    const normalized = deepNormalize(d);
    return normalized.Volume;
  }).slice(0, 10);

  return {
    // Identificação
    Ticker: obj.Ticker,
    Nome: identification.Name,
    Sector: identification.Sector,
    
    // Indicadores
    PriceSMA: indicators.PriceSMA,
    AverageVolume: indicators.AverageVolume,

    // Dados diários (para compatibilidade com detalhes)
    Prices: prices,
    Volumes: volumes,
    DailyData: days,

    // Objeto completo normalizado
    raw: obj
  };
}

async function getXmlData(queryType = 'allAssets', filters = {}) {
  return new Promise((resolve, reject) => {
    try {
      console.log(`[CONNECT] A Conectar ao XML Service gRPC em ${XML_SERVICE_GRPC_URL}...`);
      
      const client = getGrpcClient();
      
      // Diferentes XPath queries disponíveis
      const xpathQueries = {
        // 1. Simple Data Extraction
        allAssets: '//Asset',
        allNames: '//Identification/Name/text()',
        allTickers: '//Asset/@Ticker',
        allSMAPrices: '//Indicators/PriceSMA/text()',
        allSectors: '//Asset/Identification/Sector/text()',
        
        // 2. Conditional Queries (Filters)
        technologySector: "//Asset[Identification/Sector='Technology']/Identification/Name/text()",
        financialSector: "//Asset[Identification/Sector='Financial Services']/Identification/Name/text()",
        smaAbove150: '//Asset[Indicators/PriceSMA > 150]/@Ticker',
        smaBelow100: '//Asset[Indicators/PriceSMA < 100]',
        
        // 3. Specific Asset Queries
        aapl: "//Asset[@Ticker='AAPL']",
        tsla: "//Asset[@Ticker='TSLA']",
        msft: "//Asset[@Ticker='MSFT']",
        appleByName: "//Asset[Identification/Name='Apple Inc.']",
        
        // 4. Hierarchical/Deep Queries
        day10Prices: "//Day[@index='10']/ClosingPrice/text()",
        aaplVolumes: "//Asset[@Ticker='AAPL']/DailyData/Day/Volume/text()",
        tslaDay1Currency: "//Asset[@Ticker='TSLA']/DailyData/Day[@index='1']/ClosingPrice/@Currency",
        allDailyPrices: '//DailyData/Day/ClosingPrice/text()',
        
        // 5. Advanced/Structural Queries
        countAssets: 'count(//Asset)',
        jobID: '/MarketReport/@JobID',
        reportMetadata: '/MarketReport'
      };
      
      // Se queryType começa com '//' ou '/', é uma query XPath custom
      let xpathQuery;
      if (queryType.startsWith('//') || queryType.startsWith('/') || queryType.startsWith('count(')) {
        xpathQuery = queryType;
        console.log(`[XPATH CUSTOM] Query: ${xpathQuery}`);
      } else {
        // Selecionar XPath baseado no preset ou usar padrão
        xpathQuery = xpathQueries[queryType] || xpathQueries.allAssets;
        console.log(`[XPATH PRESET] Tipo: ${queryType} | Query: ${xpathQuery}`);
      }
      
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
        resolve([]);
      });
      
    } catch (error) {
      console.error('[ERROR] Erro ao conectar XML Service:', error.message);
      resolve([]);
    }
  });
}

module.exports = { getXmlData };
