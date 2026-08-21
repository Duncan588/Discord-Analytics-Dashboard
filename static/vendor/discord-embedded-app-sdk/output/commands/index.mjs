export { Commands } from '../schema/common.mjs';
import { authorize } from './authorize.mjs';
import { captureLog } from './captureLog.mjs';
import { encourageHardwareAcceleration } from './encourageHardwareAcceleration.mjs';
import { getChannel } from './getChannel.mjs';
import { getEntitlements } from './getEntitlements.mjs';
import { getSkus } from './getSkus.mjs';
import { getChannelPermissions } from './getChannelPermissions.mjs';
import { getPlatformBehaviors } from './getPlatformBehaviors.mjs';
import { openExternalLink } from './openExternalLink.mjs';
import { openInviteDialog } from './openInviteDialog.mjs';
import { setActivity } from './setActivity.mjs';
import { setConfig } from './setConfig.mjs';
import { setOrientationLockState } from './setOrientationLockState.mjs';
import { startPurchase } from './startPurchase.mjs';
import { userSettingsGetLocale } from './userSettingsGetLocale.mjs';
import { authenticate } from './authenticate.mjs';
import { getActivityInstanceConnectedParticipants } from './getActivityInstanceConnectedParticipants.mjs';
import { getQuest } from './getQuest.mjs';
import { getQuestEnrollmentStatus } from './getQuestEnrollmentStatus.mjs';
import { getRelationships } from './getRelationships.mjs';
import { getUser } from './getUser.mjs';
import { initiateImageUpload } from './initiateImageUpload.mjs';
import { inviteUserEmbedded } from './inviteUserEmbedded.mjs';
import { openShareMomentDialog } from './openShareMomentDialog.mjs';
import { questStartTimer } from './questStartTimer.mjs';
import { requestProxyTicketRefresh } from './requestProxyTicketRefresh.mjs';
import { shareInteraction } from './shareInteraction.mjs';
import { shareLink } from './shareLink.mjs';

function commands(sendCommand) {
    return {
        authorize: authorize(sendCommand),
        captureLog: captureLog(sendCommand),
        encourageHardwareAcceleration: encourageHardwareAcceleration(sendCommand),
        getChannel: getChannel(sendCommand),
        getChannelPermissions: getChannelPermissions(sendCommand),
        getEntitlements: getEntitlements(sendCommand),
        getPlatformBehaviors: getPlatformBehaviors(sendCommand),
        getSkus: getSkus(sendCommand),
        openExternalLink: openExternalLink(sendCommand),
        openInviteDialog: openInviteDialog(sendCommand),
        setActivity: setActivity(sendCommand),
        setConfig: setConfig(sendCommand),
        setOrientationLockState: setOrientationLockState(sendCommand),
        startPurchase: startPurchase(sendCommand),
        userSettingsGetLocale: userSettingsGetLocale(sendCommand),
        // Backward compatibility - getInstanceConnectedParticipants is an alias for getActivityInstanceConnectedParticipants
        getInstanceConnectedParticipants: getActivityInstanceConnectedParticipants(sendCommand),
        // START-GENERATED-SECTION
        authenticate: authenticate(sendCommand),
        getActivityInstanceConnectedParticipants: getActivityInstanceConnectedParticipants(sendCommand),
        getQuest: getQuest(sendCommand),
        getQuestEnrollmentStatus: getQuestEnrollmentStatus(sendCommand),
        getRelationships: getRelationships(sendCommand),
        getUser: getUser(sendCommand),
        initiateImageUpload: initiateImageUpload(sendCommand),
        inviteUserEmbedded: inviteUserEmbedded(sendCommand),
        openShareMomentDialog: openShareMomentDialog(sendCommand),
        questStartTimer: questStartTimer(sendCommand),
        requestProxyTicketRefresh: requestProxyTicketRefresh(sendCommand),
        shareInteraction: shareInteraction(sendCommand),
        shareLink: shareLink(sendCommand),
        // END-GENERATED-SECTION
    };
}

export { commands as default };
