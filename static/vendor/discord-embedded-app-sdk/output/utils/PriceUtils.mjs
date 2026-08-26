import Decimal from '../lib/decimal.js-light/decimal.mjs';
import { CurrencyExponents } from './PriceConstants.mjs';

function formatPrice(price, locale = 'en-US') {
    const { amount, currency } = price;
    const formatter = Intl.NumberFormat(locale, { style: 'currency', currency });
    return formatter.format(convertToMajorCurrencyUnits(amount, currency));
}
function convertToMajorCurrencyUnits(minorUnitValue, currency) {
    const exponent = CurrencyExponents[currency];
    if (exponent == null) {
        console.warn(`Unexpected currency ${currency}`);
        return minorUnitValue;
    }
    const minorUnit = new Decimal(minorUnitValue);
    return minorUnit.dividedBy(10 ** exponent).toNumber();
}
var PriceUtils = {
    formatPrice,
};

export { PriceUtils as default };
