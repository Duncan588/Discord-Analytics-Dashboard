import { Command } from '../generated/schemas.mjs';
import { schemaCommandFactory } from '../utils/commandFactory.mjs';

const requestProxyTicketRefresh = schemaCommandFactory(Command.REQUEST_PROXY_TICKET_REFRESH);

export { requestProxyTicketRefresh };
