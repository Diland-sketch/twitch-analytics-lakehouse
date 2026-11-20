import {defineConfig} from '@playwright/test'
export default defineConfig({testDir:'./e2e',workers:1,timeout:180000,use:{baseURL:process.env.I5_BASE_URL||'http://127.0.0.1:5173',channel:process.env.PW_CHANNEL||'chrome',headless:true,viewport:{width:1440,height:1080},trace:'retain-on-failure',screenshot:'only-on-failure'},reporter:[['list'],['json',{outputFile:'test-results/results.json'}]]})
