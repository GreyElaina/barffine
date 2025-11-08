use async_graphql::{Enum, EnumType, SimpleObject, Union};

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(name = "ErrorNames")]
pub enum ErrorNames {
    #[graphql(name = "ACCESS_DENIED")]
    AccessDenied,
    #[graphql(name = "ACTION_FORBIDDEN")]
    ActionForbidden,
    #[graphql(name = "ACTION_FORBIDDEN_ON_NON_TEAM_WORKSPACE")]
    ActionForbiddenOnNonTeamWorkspace,
    #[graphql(name = "ALREADY_IN_SPACE")]
    AlreadyInSpace,
    #[graphql(name = "AUTHENTICATION_REQUIRED")]
    AuthenticationRequired,
    #[graphql(name = "BAD_REQUEST")]
    BadRequest,
    #[graphql(name = "BLOB_NOT_FOUND")]
    BlobNotFound,
    #[graphql(name = "BLOB_QUOTA_EXCEEDED")]
    BlobQuotaExceeded,
    #[graphql(name = "CANNOT_DELETE_ACCOUNT_WITH_OWNED_TEAM_WORKSPACE")]
    CannotDeleteAccountWithOwnedTeamWorkspace,
    #[graphql(name = "CANNOT_DELETE_ALL_ADMIN_ACCOUNT")]
    CannotDeleteAllAdminAccount,
    #[graphql(name = "CANNOT_DELETE_OWN_ACCOUNT")]
    CannotDeleteOwnAccount,
    #[graphql(name = "CANT_UPDATE_ONETIME_PAYMENT_SUBSCRIPTION")]
    CantUpdateOnetimePaymentSubscription,
    #[graphql(name = "CAN_NOT_BATCH_GRANT_DOC_OWNER_PERMISSIONS")]
    CanNotBatchGrantDocOwnerPermissions,
    #[graphql(name = "CAN_NOT_REVOKE_YOURSELF")]
    CanNotRevokeYourself,
    #[graphql(name = "CAPTCHA_VERIFICATION_FAILED")]
    CaptchaVerificationFailed,
    #[graphql(name = "COMMENT_ATTACHMENT_NOT_FOUND")]
    CommentAttachmentNotFound,
    #[graphql(name = "COMMENT_ATTACHMENT_QUOTA_EXCEEDED")]
    CommentAttachmentQuotaExceeded,
    #[graphql(name = "COMMENT_NOT_FOUND")]
    CommentNotFound,
    #[graphql(name = "COPILOT_ACTION_TAKEN")]
    CopilotActionTaken,
    #[graphql(name = "COPILOT_CONTEXT_FILE_NOT_SUPPORTED")]
    CopilotContextFileNotSupported,
    #[graphql(name = "COPILOT_DOCS_NOT_FOUND")]
    CopilotDocsNotFound,
    #[graphql(name = "COPILOT_DOC_NOT_FOUND")]
    CopilotDocNotFound,
    #[graphql(name = "COPILOT_EMBEDDING_DISABLED")]
    CopilotEmbeddingDisabled,
    #[graphql(name = "COPILOT_EMBEDDING_UNAVAILABLE")]
    CopilotEmbeddingUnavailable,
    #[graphql(name = "COPILOT_FAILED_TO_ADD_WORKSPACE_FILE_EMBEDDING")]
    CopilotFailedToAddWorkspaceFileEmbedding,
    #[graphql(name = "COPILOT_FAILED_TO_CREATE_MESSAGE")]
    CopilotFailedToCreateMessage,
    #[graphql(name = "COPILOT_FAILED_TO_GENERATE_EMBEDDING")]
    CopilotFailedToGenerateEmbedding,
    #[graphql(name = "COPILOT_FAILED_TO_GENERATE_TEXT")]
    CopilotFailedToGenerateText,
    #[graphql(name = "COPILOT_FAILED_TO_MATCH_CONTEXT")]
    CopilotFailedToMatchContext,
    #[graphql(name = "COPILOT_FAILED_TO_MATCH_GLOBAL_CONTEXT")]
    CopilotFailedToMatchGlobalContext,
    #[graphql(name = "COPILOT_FAILED_TO_MODIFY_CONTEXT")]
    CopilotFailedToModifyContext,
    #[graphql(name = "COPILOT_INVALID_CONTEXT")]
    CopilotInvalidContext,
    #[graphql(name = "COPILOT_MESSAGE_NOT_FOUND")]
    CopilotMessageNotFound,
    #[graphql(name = "COPILOT_PROMPT_INVALID")]
    CopilotPromptInvalid,
    #[graphql(name = "COPILOT_PROMPT_NOT_FOUND")]
    CopilotPromptNotFound,
    #[graphql(name = "COPILOT_PROVIDER_NOT_SUPPORTED")]
    CopilotProviderNotSupported,
    #[graphql(name = "COPILOT_PROVIDER_SIDE_ERROR")]
    CopilotProviderSideError,
    #[graphql(name = "COPILOT_QUOTA_EXCEEDED")]
    CopilotQuotaExceeded,
    #[graphql(name = "COPILOT_SESSION_DELETED")]
    CopilotSessionDeleted,
    #[graphql(name = "COPILOT_SESSION_INVALID_INPUT")]
    CopilotSessionInvalidInput,
    #[graphql(name = "COPILOT_SESSION_NOT_FOUND")]
    CopilotSessionNotFound,
    #[graphql(name = "COPILOT_TRANSCRIPTION_AUDIO_NOT_PROVIDED")]
    CopilotTranscriptionAudioNotProvided,
    #[graphql(name = "COPILOT_TRANSCRIPTION_JOB_EXISTS")]
    CopilotTranscriptionJobExists,
    #[graphql(name = "COPILOT_TRANSCRIPTION_JOB_NOT_FOUND")]
    CopilotTranscriptionJobNotFound,
    #[graphql(name = "CUSTOMER_PORTAL_CREATE_FAILED")]
    CustomerPortalCreateFailed,
    #[graphql(name = "DOC_ACTION_DENIED")]
    DocActionDenied,
    #[graphql(name = "DOC_DEFAULT_ROLE_CAN_NOT_BE_OWNER")]
    DocDefaultRoleCanNotBeOwner,
    #[graphql(name = "DOC_HISTORY_NOT_FOUND")]
    DocHistoryNotFound,
    #[graphql(name = "DOC_IS_NOT_PUBLIC")]
    DocIsNotPublic,
    #[graphql(name = "DOC_NOT_FOUND")]
    DocNotFound,
    #[graphql(name = "DOC_UPDATE_BLOCKED")]
    DocUpdateBlocked,
    #[graphql(name = "EARLY_ACCESS_REQUIRED")]
    EarlyAccessRequired,
    #[graphql(name = "EMAIL_ALREADY_USED")]
    EmailAlreadyUsed,
    #[graphql(name = "EMAIL_SERVICE_NOT_CONFIGURED")]
    EmailServiceNotConfigured,
    #[graphql(name = "EMAIL_TOKEN_NOT_FOUND")]
    EmailTokenNotFound,
    #[graphql(name = "EMAIL_VERIFICATION_REQUIRED")]
    EmailVerificationRequired,
    #[graphql(name = "EXPECT_TO_GRANT_DOC_USER_ROLES")]
    ExpectToGrantDocUserRoles,
    #[graphql(name = "EXPECT_TO_PUBLISH_DOC")]
    ExpectToPublishDoc,
    #[graphql(name = "EXPECT_TO_REVOKE_DOC_USER_ROLES")]
    ExpectToRevokeDocUserRoles,
    #[graphql(name = "EXPECT_TO_REVOKE_PUBLIC_DOC")]
    ExpectToRevokePublicDoc,
    #[graphql(name = "EXPECT_TO_UPDATE_DOC_USER_ROLE")]
    ExpectToUpdateDocUserRole,
    #[graphql(name = "FAILED_TO_CHECKOUT")]
    FailedToCheckout,
    #[graphql(name = "FAILED_TO_SAVE_UPDATES")]
    FailedToSaveUpdates,
    #[graphql(name = "FAILED_TO_UPSERT_SNAPSHOT")]
    FailedToUpsertSnapshot,
    #[graphql(name = "GRAPHQL_BAD_REQUEST")]
    GraphqlBadRequest,
    #[graphql(name = "HTTP_REQUEST_ERROR")]
    HttpRequestError,
    #[graphql(name = "INTERNAL_SERVER_ERROR")]
    InternalServerError,
    #[graphql(name = "INVALID_APP_CONFIG")]
    InvalidAppConfig,
    #[graphql(name = "INVALID_APP_CONFIG_INPUT")]
    InvalidAppConfigInput,
    #[graphql(name = "INVALID_AUTH_STATE")]
    InvalidAuthState,
    #[graphql(name = "INVALID_CHECKOUT_PARAMETERS")]
    InvalidCheckoutParameters,
    #[graphql(name = "INVALID_EMAIL")]
    InvalidEmail,
    #[graphql(name = "INVALID_EMAIL_TOKEN")]
    InvalidEmailToken,
    #[graphql(name = "INVALID_HISTORY_TIMESTAMP")]
    InvalidHistoryTimestamp,
    #[graphql(name = "INVALID_INDEXER_INPUT")]
    InvalidIndexerInput,
    #[graphql(name = "INVALID_INVITATION")]
    InvalidInvitation,
    #[graphql(name = "INVALID_LICENSE_SESSION_ID")]
    InvalidLicenseSessionId,
    #[graphql(name = "INVALID_LICENSE_TO_ACTIVATE")]
    InvalidLicenseToActivate,
    #[graphql(name = "INVALID_LICENSE_UPDATE_PARAMS")]
    InvalidLicenseUpdateParams,
    #[graphql(name = "INVALID_OAUTH_CALLBACK_CODE")]
    InvalidOauthCallbackCode,
    #[graphql(name = "INVALID_OAUTH_CALLBACK_STATE")]
    InvalidOauthCallbackState,
    #[graphql(name = "INVALID_OAUTH_RESPONSE")]
    InvalidOauthResponse,
    #[graphql(name = "INVALID_PASSWORD_LENGTH")]
    InvalidPasswordLength,
    #[graphql(name = "INVALID_RUNTIME_CONFIG_TYPE")]
    InvalidRuntimeConfigType,
    #[graphql(name = "INVALID_SEARCH_PROVIDER_REQUEST")]
    InvalidSearchProviderRequest,
    #[graphql(name = "INVALID_SUBSCRIPTION_PARAMETERS")]
    InvalidSubscriptionParameters,
    #[graphql(name = "LICENSE_EXPIRED")]
    LicenseExpired,
    #[graphql(name = "LICENSE_NOT_FOUND")]
    LicenseNotFound,
    #[graphql(name = "LICENSE_REVEALED")]
    LicenseRevealed,
    #[graphql(name = "LINK_EXPIRED")]
    LinkExpired,
    #[graphql(name = "MAILER_SERVICE_IS_NOT_CONFIGURED")]
    MailerServiceIsNotConfigured,
    #[graphql(name = "MANAGED_BY_APP_STORE_OR_PLAY")]
    ManagedByAppStoreOrPlay,
    #[graphql(name = "MEMBER_NOT_FOUND_IN_SPACE")]
    MemberNotFoundInSpace,
    #[graphql(name = "MEMBER_QUOTA_EXCEEDED")]
    MemberQuotaExceeded,
    #[graphql(name = "MENTION_USER_DOC_ACCESS_DENIED")]
    MentionUserDocAccessDenied,
    #[graphql(name = "MENTION_USER_ONESELF_DENIED")]
    MentionUserOneselfDenied,
    #[graphql(name = "MISSING_OAUTH_QUERY_PARAMETER")]
    MissingOauthQueryParameter,
    #[graphql(name = "NETWORK_ERROR")]
    NetworkError,
    #[graphql(name = "NEW_OWNER_IS_NOT_ACTIVE_MEMBER")]
    NewOwnerIsNotActiveMember,
    #[graphql(name = "NOTIFICATION_NOT_FOUND")]
    NotificationNotFound,
    #[graphql(name = "NOT_FOUND")]
    NotFound,
    #[graphql(name = "NOT_IN_SPACE")]
    NotInSpace,
    #[graphql(name = "NO_COPILOT_PROVIDER_AVAILABLE")]
    NoCopilotProviderAvailable,
    #[graphql(name = "NO_MORE_SEAT")]
    NoMoreSeat,
    #[graphql(name = "OAUTH_ACCOUNT_ALREADY_CONNECTED")]
    OauthAccountAlreadyConnected,
    #[graphql(name = "OAUTH_STATE_EXPIRED")]
    OauthStateExpired,
    #[graphql(name = "OWNER_CAN_NOT_LEAVE_WORKSPACE")]
    OwnerCanNotLeaveWorkspace,
    #[graphql(name = "PASSWORD_REQUIRED")]
    PasswordRequired,
    #[graphql(name = "QUERY_TOO_LONG")]
    QueryTooLong,
    #[graphql(name = "REPLY_NOT_FOUND")]
    ReplyNotFound,
    #[graphql(name = "RUNTIME_CONFIG_NOT_FOUND")]
    RuntimeConfigNotFound,
    #[graphql(name = "SAME_EMAIL_PROVIDED")]
    SameEmailProvided,
    #[graphql(name = "SAME_SUBSCRIPTION_RECURRING")]
    SameSubscriptionRecurring,
    #[graphql(name = "SEARCH_PROVIDER_NOT_FOUND")]
    SearchProviderNotFound,
    #[graphql(name = "SIGN_UP_FORBIDDEN")]
    SignUpForbidden,
    #[graphql(name = "SPACE_ACCESS_DENIED")]
    SpaceAccessDenied,
    #[graphql(name = "SPACE_NOT_FOUND")]
    SpaceNotFound,
    #[graphql(name = "SPACE_OWNER_NOT_FOUND")]
    SpaceOwnerNotFound,
    #[graphql(name = "SPACE_SHOULD_HAVE_ONLY_ONE_OWNER")]
    SpaceShouldHaveOnlyOneOwner,
    #[graphql(name = "STORAGE_QUOTA_EXCEEDED")]
    StorageQuotaExceeded,
    #[graphql(name = "SUBSCRIPTION_ALREADY_EXISTS")]
    SubscriptionAlreadyExists,
    #[graphql(name = "SUBSCRIPTION_EXPIRED")]
    SubscriptionExpired,
    #[graphql(name = "SUBSCRIPTION_HAS_BEEN_CANCELED")]
    SubscriptionHasBeenCanceled,
    #[graphql(name = "SUBSCRIPTION_HAS_NOT_BEEN_CANCELED")]
    SubscriptionHasNotBeenCanceled,
    #[graphql(name = "SUBSCRIPTION_NOT_EXISTS")]
    SubscriptionNotExists,
    #[graphql(name = "SUBSCRIPTION_PLAN_NOT_FOUND")]
    SubscriptionPlanNotFound,
    #[graphql(name = "TOO_MANY_REQUEST")]
    TooManyRequest,
    #[graphql(name = "UNKNOWN_OAUTH_PROVIDER")]
    UnknownOauthProvider,
    #[graphql(name = "UNSPLASH_IS_NOT_CONFIGURED")]
    UnsplashIsNotConfigured,
    #[graphql(name = "UNSUPPORTED_CLIENT_VERSION")]
    UnsupportedClientVersion,
    #[graphql(name = "UNSUPPORTED_SUBSCRIPTION_PLAN")]
    UnsupportedSubscriptionPlan,
    #[graphql(name = "USER_AVATAR_NOT_FOUND")]
    UserAvatarNotFound,
    #[graphql(name = "USER_NOT_FOUND")]
    UserNotFound,
    #[graphql(name = "VALIDATION_ERROR")]
    ValidationError,
    #[graphql(name = "VERSION_REJECTED")]
    VersionRejected,
    #[graphql(name = "WORKSPACE_ID_REQUIRED_FOR_TEAM_SUBSCRIPTION")]
    WorkspaceIdRequiredForTeamSubscription,
    #[graphql(name = "WORKSPACE_ID_REQUIRED_TO_UPDATE_TEAM_SUBSCRIPTION")]
    WorkspaceIdRequiredToUpdateTeamSubscription,
    #[graphql(name = "WORKSPACE_LICENSE_ALREADY_EXISTS")]
    WorkspaceLicenseAlreadyExists,
    #[graphql(name = "WORKSPACE_PERMISSION_NOT_FOUND")]
    WorkspacePermissionNotFound,
    #[graphql(name = "WRONG_SIGN_IN_CREDENTIALS")]
    WrongSignInCredentials,
    #[graphql(name = "WRONG_SIGN_IN_METHOD")]
    WrongSignInMethod,
}
impl ErrorNames {
    pub fn as_str(&self) -> &'static str {
        Self::items()
            .iter()
            .find(|item| &item.value == self)
            .map(|item| item.name)
            .unwrap_or("UNKNOWN_ERROR")
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "AlreadyInSpaceDataType")]
pub struct AlreadyInSpaceDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "BlobNotFoundDataType")]
pub struct BlobNotFoundDataType {
    #[graphql(name = "blobId")]
    pub blob_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotContextFileNotSupportedDataType")]
pub struct CopilotContextFileNotSupportedDataType {
    #[graphql(name = "fileName")]
    pub file_name: String,
    pub message: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotDocNotFoundDataType")]
pub struct CopilotDocNotFoundDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotFailedToAddWorkspaceFileEmbeddingDataType")]
pub struct CopilotFailedToAddWorkspaceFileEmbeddingDataType {
    pub message: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotFailedToGenerateEmbeddingDataType")]
pub struct CopilotFailedToGenerateEmbeddingDataType {
    pub message: String,
    pub provider: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotFailedToMatchContextDataType")]
pub struct CopilotFailedToMatchContextDataType {
    pub content: String,
    #[graphql(name = "contextId")]
    pub context_id: String,
    pub message: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotFailedToMatchGlobalContextDataType")]
pub struct CopilotFailedToMatchGlobalContextDataType {
    pub content: String,
    pub message: String,
    #[graphql(name = "workspaceId")]
    pub workspace_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotFailedToModifyContextDataType")]
pub struct CopilotFailedToModifyContextDataType {
    #[graphql(name = "contextId")]
    pub context_id: String,
    pub message: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotInvalidContextDataType")]
pub struct CopilotInvalidContextDataType {
    #[graphql(name = "contextId")]
    pub context_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotMessageNotFoundDataType")]
pub struct CopilotMessageNotFoundDataType {
    #[graphql(name = "messageId")]
    pub message_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotPromptNotFoundDataType")]
pub struct CopilotPromptNotFoundDataType {
    pub name: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotProviderNotSupportedDataType")]
pub struct CopilotProviderNotSupportedDataType {
    pub kind: String,
    pub provider: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "CopilotProviderSideErrorDataType")]
pub struct CopilotProviderSideErrorDataType {
    pub kind: String,
    pub message: String,
    pub provider: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "DocActionDeniedDataType")]
pub struct DocActionDeniedDataType {
    pub action: String,
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "DocHistoryNotFoundDataType")]
pub struct DocHistoryNotFoundDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
    pub timestamp: i32,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "DocNotFoundDataType")]
pub struct DocNotFoundDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "DocUpdateBlockedDataType")]
pub struct DocUpdateBlockedDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "ExpectToGrantDocUserRolesDataType")]
pub struct ExpectToGrantDocUserRolesDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "ExpectToRevokeDocUserRolesDataType")]
pub struct ExpectToRevokeDocUserRolesDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "ExpectToUpdateDocUserRoleDataType")]
pub struct ExpectToUpdateDocUserRoleDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "GraphqlBadRequestDataType")]
pub struct GraphqlBadRequestDataType {
    pub code: String,
    pub message: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "HttpRequestErrorDataType")]
pub struct HttpRequestErrorDataType {
    pub message: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidAppConfigDataType")]
pub struct InvalidAppConfigDataType {
    pub hint: String,
    pub key: String,
    pub module: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidAppConfigInputDataType")]
pub struct InvalidAppConfigInputDataType {
    pub message: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidEmailDataType")]
pub struct InvalidEmailDataType {
    pub email: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidHistoryTimestampDataType")]
pub struct InvalidHistoryTimestampDataType {
    pub timestamp: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidIndexerInputDataType")]
pub struct InvalidIndexerInputDataType {
    pub reason: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidLicenseToActivateDataType")]
pub struct InvalidLicenseToActivateDataType {
    pub reason: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidLicenseUpdateParamsDataType")]
pub struct InvalidLicenseUpdateParamsDataType {
    pub reason: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidOauthCallbackCodeDataType")]
pub struct InvalidOauthCallbackCodeDataType {
    pub body: String,
    pub status: i32,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidOauthResponseDataType")]
pub struct InvalidOauthResponseDataType {
    pub reason: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidPasswordLengthDataType")]
pub struct InvalidPasswordLengthDataType {
    pub max: i32,
    pub min: i32,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidRuntimeConfigTypeDataType")]
pub struct InvalidRuntimeConfigTypeDataType {
    pub get: String,
    pub key: String,
    pub want: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "InvalidSearchProviderRequestDataType")]
pub struct InvalidSearchProviderRequestDataType {
    pub reason: String,
    pub r#type: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "MemberNotFoundInSpaceDataType")]
pub struct MemberNotFoundInSpaceDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "MentionUserDocAccessDeniedDataType")]
pub struct MentionUserDocAccessDeniedDataType {
    #[graphql(name = "docId")]
    pub doc_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "MissingOauthQueryParameterDataType")]
pub struct MissingOauthQueryParameterDataType {
    pub name: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "NoCopilotProviderAvailableDataType")]
pub struct NoCopilotProviderAvailableDataType {
    #[graphql(name = "modelId")]
    pub model_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "NoMoreSeatDataType")]
pub struct NoMoreSeatDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "NotInSpaceDataType")]
pub struct NotInSpaceDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "QueryTooLongDataType")]
pub struct QueryTooLongDataType {
    pub max: i32,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "RuntimeConfigNotFoundDataType")]
pub struct RuntimeConfigNotFoundDataType {
    pub key: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SameSubscriptionRecurringDataType")]
pub struct SameSubscriptionRecurringDataType {
    pub recurring: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SpaceAccessDeniedDataType")]
pub struct SpaceAccessDeniedDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SpaceNotFoundDataType")]
pub struct SpaceNotFoundDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SpaceOwnerNotFoundDataType")]
pub struct SpaceOwnerNotFoundDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SpaceShouldHaveOnlyOneOwnerDataType")]
pub struct SpaceShouldHaveOnlyOneOwnerDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SubscriptionAlreadyExistsDataType")]
pub struct SubscriptionAlreadyExistsDataType {
    pub plan: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SubscriptionNotExistsDataType")]
pub struct SubscriptionNotExistsDataType {
    pub plan: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "SubscriptionPlanNotFoundDataType")]
pub struct SubscriptionPlanNotFoundDataType {
    pub plan: String,
    pub recurring: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UnknownOauthProviderDataType")]
pub struct UnknownOauthProviderDataType {
    pub name: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UnsupportedClientVersionDataType")]
pub struct UnsupportedClientVersionDataType {
    #[graphql(name = "clientVersion")]
    pub client_version: String,
    #[graphql(name = "requiredVersion")]
    pub required_version: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "UnsupportedSubscriptionPlanDataType")]
pub struct UnsupportedSubscriptionPlanDataType {
    pub plan: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "ValidationErrorDataType")]
pub struct ValidationErrorDataType {
    pub errors: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "VersionRejectedDataType")]
pub struct VersionRejectedDataType {
    #[graphql(name = "serverVersion")]
    pub server_version: String,
    pub version: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "WorkspacePermissionNotFoundDataType")]
pub struct WorkspacePermissionNotFoundDataType {
    #[graphql(name = "spaceId")]
    pub space_id: String,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "WrongSignInCredentialsDataType")]
pub struct WrongSignInCredentialsDataType {
    pub email: String,
}
#[derive(Union)]
pub enum ErrorDataUnion {
    AlreadyInSpaceDataType(AlreadyInSpaceDataType),
    BlobNotFoundDataType(BlobNotFoundDataType),
    CopilotContextFileNotSupportedDataType(CopilotContextFileNotSupportedDataType),
    CopilotDocNotFoundDataType(CopilotDocNotFoundDataType),
    CopilotFailedToAddWorkspaceFileEmbeddingDataType(
        CopilotFailedToAddWorkspaceFileEmbeddingDataType,
    ),
    CopilotFailedToGenerateEmbeddingDataType(CopilotFailedToGenerateEmbeddingDataType),
    CopilotFailedToMatchContextDataType(CopilotFailedToMatchContextDataType),
    CopilotFailedToMatchGlobalContextDataType(CopilotFailedToMatchGlobalContextDataType),
    CopilotFailedToModifyContextDataType(CopilotFailedToModifyContextDataType),
    CopilotInvalidContextDataType(CopilotInvalidContextDataType),
    CopilotMessageNotFoundDataType(CopilotMessageNotFoundDataType),
    CopilotPromptNotFoundDataType(CopilotPromptNotFoundDataType),
    CopilotProviderNotSupportedDataType(CopilotProviderNotSupportedDataType),
    CopilotProviderSideErrorDataType(CopilotProviderSideErrorDataType),
    DocActionDeniedDataType(DocActionDeniedDataType),
    DocHistoryNotFoundDataType(DocHistoryNotFoundDataType),
    DocNotFoundDataType(DocNotFoundDataType),
    DocUpdateBlockedDataType(DocUpdateBlockedDataType),
    ExpectToGrantDocUserRolesDataType(ExpectToGrantDocUserRolesDataType),
    ExpectToRevokeDocUserRolesDataType(ExpectToRevokeDocUserRolesDataType),
    ExpectToUpdateDocUserRoleDataType(ExpectToUpdateDocUserRoleDataType),
    GraphqlBadRequestDataType(GraphqlBadRequestDataType),
    HttpRequestErrorDataType(HttpRequestErrorDataType),
    InvalidAppConfigDataType(InvalidAppConfigDataType),
    InvalidAppConfigInputDataType(InvalidAppConfigInputDataType),
    InvalidEmailDataType(InvalidEmailDataType),
    InvalidHistoryTimestampDataType(InvalidHistoryTimestampDataType),
    InvalidIndexerInputDataType(InvalidIndexerInputDataType),
    InvalidLicenseToActivateDataType(InvalidLicenseToActivateDataType),
    InvalidLicenseUpdateParamsDataType(InvalidLicenseUpdateParamsDataType),
    InvalidOauthCallbackCodeDataType(InvalidOauthCallbackCodeDataType),
    InvalidOauthResponseDataType(InvalidOauthResponseDataType),
    InvalidPasswordLengthDataType(InvalidPasswordLengthDataType),
    InvalidRuntimeConfigTypeDataType(InvalidRuntimeConfigTypeDataType),
    InvalidSearchProviderRequestDataType(InvalidSearchProviderRequestDataType),
    MemberNotFoundInSpaceDataType(MemberNotFoundInSpaceDataType),
    MentionUserDocAccessDeniedDataType(MentionUserDocAccessDeniedDataType),
    MissingOauthQueryParameterDataType(MissingOauthQueryParameterDataType),
    NoCopilotProviderAvailableDataType(NoCopilotProviderAvailableDataType),
    NoMoreSeatDataType(NoMoreSeatDataType),
    NotInSpaceDataType(NotInSpaceDataType),
    QueryTooLongDataType(QueryTooLongDataType),
    RuntimeConfigNotFoundDataType(RuntimeConfigNotFoundDataType),
    SameSubscriptionRecurringDataType(SameSubscriptionRecurringDataType),
    SpaceAccessDeniedDataType(SpaceAccessDeniedDataType),
    SpaceNotFoundDataType(SpaceNotFoundDataType),
    SpaceOwnerNotFoundDataType(SpaceOwnerNotFoundDataType),
    SpaceShouldHaveOnlyOneOwnerDataType(SpaceShouldHaveOnlyOneOwnerDataType),
    SubscriptionAlreadyExistsDataType(SubscriptionAlreadyExistsDataType),
    SubscriptionNotExistsDataType(SubscriptionNotExistsDataType),
    SubscriptionPlanNotFoundDataType(SubscriptionPlanNotFoundDataType),
    UnknownOauthProviderDataType(UnknownOauthProviderDataType),
    UnsupportedClientVersionDataType(UnsupportedClientVersionDataType),
    UnsupportedSubscriptionPlanDataType(UnsupportedSubscriptionPlanDataType),
    ValidationErrorDataType(ValidationErrorDataType),
    VersionRejectedDataType(VersionRejectedDataType),
    WorkspacePermissionNotFoundDataType(WorkspacePermissionNotFoundDataType),
    WrongSignInCredentialsDataType(WrongSignInCredentialsDataType),
}
