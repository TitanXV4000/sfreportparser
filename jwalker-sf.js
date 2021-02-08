/* Logs into salesforce and returns the page */
exports.login = run = async (browser, url, username, password, timeout, logger) => {
  const page = await browser.newPage();
  logger.debug("Blank page loaded.");

  /* Go to the page and wait for it to load */
  await page.goto(url, { waitUntil: 'networkidle2' });
  logger.debug("Salesforce initial auth page loaded.");

  /* Click on the SSO button */
  await Promise.all([
    page.click('#idp_section_buttons > button > span'),
    waitForNetworkIdle(page, 2000, 0),
    logger.debug("Navigating to SSO page."),
  ]);

  /* Enter username/password */
  logger.debug("Will now enter username/password...");
  await Promise.all([
    logger.silly("In await Promise.all..."),
    await page.type('#username', username),
    await page.type('#password', password),
    await page.keyboard.press('Enter'),
    logger.info("Logged in to Salesforce. Please wait " + timeout / 1000 + " seconds..."),
    await sleep(timeout),
    logger.debug("calling waitForNetworkIdle"),
    waitForNetworkIdle(page, 1000, 0),
    logger.debug("Salesforce case page loaded."),
  ]);

  return page;
};


async function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}


/* Use if 500ms timeout of 'networkidleX' is insufficient */
var waitForNetworkIdle = exports.waitForNetworkIdle = function (page, timeout, maxInflightRequests = 0) {
  page.on('request', onRequestStarted);
  page.on('requestfinished', onRequestFinished);
  page.on('requestfailed', onRequestFinished);

  let inflight = 0;
  let fulfill;
  let promise = new Promise(x => fulfill = x);
  let timeoutId = setTimeout(onTimeoutDone, timeout);
  return promise;

  function onTimeoutDone() {
    page.removeListener('request', onRequestStarted);
    page.removeListener('requestfinished', onRequestFinished);
    page.removeListener('requestfailed', onRequestFinished);
    fulfill();
  }

  function onRequestStarted() {
    ++inflight;
    if (inflight > maxInflightRequests)
      clearTimeout(timeoutId);
  }
  
  function onRequestFinished() {
    if (inflight === 0)
      return;
    --inflight;
    if (inflight === maxInflightRequests)
      timeoutId = setTimeout(onTimeoutDone, timeout);
  }
}