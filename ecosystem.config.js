module.exports = {
  apps: [{
    name: 'market-backend',
    script: 'server.js',
    instances: 'max',
    exec_mode: 'cluster',
    env: { NODE_ENV: 'production' }
  }]
};