/**
 * DingTalk Channel Plugin for Moltbot
 *
 * 通过钉钉 Stream 模式连接，支持 AI Card 流式响应。
 * 完整接入 Moltbot 消息处理管道。
 */

import { DWClient, TOPIC_ROBOT } from 'dingtalk-stream';
import axios from 'axios';
import type { ClawdbotPluginApi, PluginRuntime, ClawdbotConfig } from 'clawdbot/plugin-sdk';

// ============ 常量 ============

export const id = 'dingtalk-connector';

let runtime: PluginRuntime | null = null;

function getRuntime(): PluginRuntime {
  if (!runtime) throw new Error('DingTalk runtime not initialized');
  return runtime;
}

// ============ Session 管理 ============

/** 用户会话状态：记录最后活跃时间和当前 session 标识 */
interface UserSession {
  lastActivity: number;
  sessionId: string;  // 格式: dingtalk-connector:<senderId> 或 dingtalk-connector:<senderId>:<timestamp>
}

/** 用户会话缓存 Map<senderId, UserSession> */
const userSessions = new Map<string, UserSession>();

/** 新会话触发命令 */
const NEW_SESSION_COMMANDS = ['/new', '/reset', '/clear', '新会话', '重新开始', '清空对话'];

/** 检查消息是否是新会话命令 */
function isNewSessionCommand(text: string): boolean {
  const trimmed = text.trim().toLowerCase();
  return NEW_SESSION_COMMANDS.some(cmd => trimmed === cmd.toLowerCase());
}

/** 获取或创建用户 session key */
function getSessionKey(
  senderId: string,
  forceNew: boolean,
  sessionTimeout: number,
  log?: any,
): { sessionKey: string; isNew: boolean } {
  const now = Date.now();
  const existing = userSessions.get(senderId);

  // 强制新会话
  if (forceNew) {
    const sessionId = `dingtalk-connector:${senderId}:${now}`;
    userSessions.set(senderId, { lastActivity: now, sessionId });
    log?.info?.(`[DingTalk][Session] 用户主动开启新会话: ${senderId}`);
    return { sessionKey: sessionId, isNew: true };
  }

  // 检查超时
  if (existing) {
    const elapsed = now - existing.lastActivity;
    if (elapsed > sessionTimeout) {
      const sessionId = `dingtalk-connector:${senderId}:${now}`;
      userSessions.set(senderId, { lastActivity: now, sessionId });
      log?.info?.(`[DingTalk][Session] 会话超时(${Math.round(elapsed / 60000)}分钟)，自动开启新会话: ${senderId}`);
      return { sessionKey: sessionId, isNew: true };
    }
    // 更新活跃时间
    existing.lastActivity = now;
    return { sessionKey: existing.sessionId, isNew: false };
  }

  // 首次会话
  const sessionId = `dingtalk-connector:${senderId}`;
  userSessions.set(senderId, { lastActivity: now, sessionId });
  log?.info?.(`[DingTalk][Session] 新用户首次会话: ${senderId}`);
  return { sessionKey: sessionId, isNew: false };
}

// ============ Access Token 缓存 ============

let accessToken: string | null = null;
let accessTokenExpiry = 0;

async function getAccessToken(config: any): Promise<string> {
  const now = Date.now();
  if (accessToken && accessTokenExpiry > now + 60_000) {
    return accessToken;
  }

  const response = await axios.post('https://api.dingtalk.com/v1.0/oauth2/accessToken', {
    appKey: config.clientId,
    appSecret: config.clientSecret,
  });

  accessToken = response.data.accessToken;
  accessTokenExpiry = now + (response.data.expireIn * 1000);
  return accessToken!;
}

// ============ 配置工具 ============

function getConfig(cfg: ClawdbotConfig) {
  return (cfg?.channels as any)?.['dingtalk-connector'] || {};
}

function isConfigured(cfg: ClawdbotConfig): boolean {
  const config = getConfig(cfg);
  return Boolean(config.clientId && config.clientSecret);
}

// ============ 钉钉图片上传 ============

async function getOapiAccessToken(config: any): Promise<string | null> {
  try {
    const resp = await axios.get('https://oapi.dingtalk.com/gettoken', {
      params: { appkey: config.clientId, appsecret: config.clientSecret },
    });
    if (resp.data?.errcode === 0) return resp.data.access_token;
    return null;
  } catch {
    return null;
  }
}

function buildMediaSystemPrompt(): string {
  return `## 钉钉图片显示规则

你正在钉钉中与用户对话。显示图片时，直接使用本地文件路径，系统会自动上传处理。

### 正确方式
\`\`\`markdown
![描述](file:///path/to/image.jpg)
![描述](/tmp/screenshot.png)
![描述](/Users/xxx/photo.jpg)
\`\`\`

### 禁止
- 不要自己执行 curl 上传
- 不要猜测或构造 URL
- 不要使用 https://oapi.dingtalk.com/... 这类地址

直接输出本地路径即可，系统会自动上传到钉钉。`;
}

// ============ 图片后处理：自动上传本地图片到钉钉 ============

/**
 * 匹配 markdown 图片中的本地文件路径（跨平台）：
 * - ![alt](file:///path/to/image.jpg)
 * - ![alt](MEDIA:/var/folders/xxx.jpg)
 * - ![alt](attachment:///path.jpg)
 * macOS:
 * - ![alt](/tmp/xxx.jpg)
 * - ![alt](/var/folders/xxx.jpg)
 * - ![alt](/Users/xxx/photo.jpg)
 * Linux:
 * - ![alt](/home/user/photo.jpg)
 * - ![alt](/root/photo.jpg)
 * Windows:
 * - ![alt](C:\Users\xxx\photo.jpg)
 * - ![alt](C:/Users/xxx/photo.jpg)
 */
const LOCAL_IMAGE_RE = /!\[([^\]]*)\]\(((?:file:\/\/\/|MEDIA:|attachment:\/\/\/)[^\s)]+|\/(?:tmp|var|private|Users|home|root)[^\s)]+|[A-Za-z]:[\\/][^\s)]+)\)/g;

/** 图片文件扩展名 */
const IMAGE_EXTENSIONS = /\.(png|jpg|jpeg|gif|bmp|webp|tiff|svg)$/i;

/**
 * 匹配纯文本中的本地图片路径（不在 markdown 图片语法中，跨平台）：
 * macOS:
 * - `/var/folders/.../screenshot.png`
 * - `/tmp/image.jpg`
 * - `/Users/xxx/photo.png`
 * Linux:
 * - `/home/user/photo.png`
 * - `/root/photo.png`
 * Windows:
 * - `C:\Users\xxx\photo.png`
 * - `C:/temp/image.jpg`
 * 支持 backtick 包裹: `path`
 */
const BARE_IMAGE_PATH_RE = /`?((?:\/(?:tmp|var|private|Users|home|root)\/[^\s`'",)]+|[A-Za-z]:[\\/][^\s`'",)]+)\.(?:png|jpg|jpeg|gif|bmp|webp))`?/gi;

/** 去掉 file:// / MEDIA: / attachment:// 前缀，得到实际的绝对路径 */
function toLocalPath(raw: string): string {
  let path = raw;
  if (path.startsWith('file://')) path = path.replace('file://', '');
  else if (path.startsWith('MEDIA:')) path = path.replace('MEDIA:', '');
  else if (path.startsWith('attachment://')) path = path.replace('attachment://', '');

  // 解码 URL 编码的路径（如中文字符 %E5%9B%BE → 图）
  try {
    path = decodeURIComponent(path);
  } catch {
    // 解码失败则保持原样
  }
  return path;
}

/** 上传本地文件到钉钉，返回 media_id（以 @ 开头） */
async function uploadToDingTalk(
  filePath: string,
  oapiToken: string,
  log?: any,
): Promise<string | null> {
  try {
    const fs = await import('fs');
    const path = await import('path');
    const FormData = (await import('form-data')).default;

    const absPath = toLocalPath(filePath);
    if (!fs.existsSync(absPath)) {
      log?.warn?.(`[DingTalk][Media] 文件不存在: ${absPath}`);
      return null;
    }

    const form = new FormData();
    form.append('media', fs.createReadStream(absPath), {
      filename: path.basename(absPath),
      contentType: 'image/jpeg',
    });

    log?.info?.(`[DingTalk][Media] 上传图片: ${absPath}`);
    const resp = await axios.post(
      `https://oapi.dingtalk.com/media/upload?access_token=${oapiToken}&type=image`,
      form,
      { headers: form.getHeaders(), timeout: 30_000 },
    );

    const mediaId = resp.data?.media_id;
    if (mediaId) {
      log?.info?.(`[DingTalk][Media] 上传成功: media_id=${mediaId}`);
      return mediaId;
    }
    log?.warn?.(`[DingTalk][Media] 上传返回无 media_id: ${JSON.stringify(resp.data)}`);
    return null;
  } catch (err: any) {
    log?.error?.(`[DingTalk][Media] 上传失败: ${err.message}`);
    return null;
  }
}

/** 扫描内容中的本地图片路径，上传到钉钉并替换为 media_id */
async function processLocalImages(
  content: string,
  oapiToken: string | null,
  log?: any,
): Promise<string> {
  if (!oapiToken) {
    log?.warn?.(`[DingTalk][Media] 无 oapiToken，跳过图片后处理`);
    return content;
  }

  let result = content;

  // 第一步：匹配 markdown 图片语法 ![alt](path)
  const mdMatches = [...content.matchAll(LOCAL_IMAGE_RE)];
  if (mdMatches.length > 0) {
    log?.info?.(`[DingTalk][Media] 检测到 ${mdMatches.length} 个 markdown 图片，开始上传...`);
    for (const match of mdMatches) {
      const [fullMatch, alt, rawPath] = match;
      const mediaId = await uploadToDingTalk(rawPath, oapiToken, log);
      if (mediaId) {
        result = result.replace(fullMatch, `![${alt}](${mediaId})`);
      }
    }
  }

  // 第二步：匹配纯文本中的本地图片路径（如 `/var/folders/.../xxx.png`）
  // 排除已被 markdown 图片语法包裹的路径
  const bareMatches = [...result.matchAll(BARE_IMAGE_PATH_RE)];
  const newBareMatches = bareMatches.filter(m => {
    // 检查这个路径是否已经在 ![...](...) 中
    const idx = m.index!;
    const before = result.slice(Math.max(0, idx - 10), idx);
    return !before.includes('](');
  });

  if (newBareMatches.length > 0) {
    log?.info?.(`[DingTalk][Media] 检测到 ${newBareMatches.length} 个纯文本图片路径，开始上传...`);
    // 从后往前替换，避免 index 偏移
    for (const match of newBareMatches.reverse()) {
      const [fullMatch, rawPath] = match;
      log?.info?.(`[DingTalk][Media] 纯文本图片: "${fullMatch}" -> path="${rawPath}"`);
      const mediaId = await uploadToDingTalk(rawPath, oapiToken, log);
      if (mediaId) {
        const replacement = `![](${mediaId})`;
        result = result.slice(0, match.index!) + result.slice(match.index!).replace(fullMatch, replacement);
        log?.info?.(`[DingTalk][Media] 替换纯文本路径为图片: ${replacement}`);
      }
    }
  }

  if (mdMatches.length === 0 && newBareMatches.length === 0) {
    log?.info?.(`[DingTalk][Media] 未检测到本地图片路径`);
  }

  return result;
}

// ============ AI Card Streaming ============

const DINGTALK_API = 'https://api.dingtalk.com';
const AI_CARD_TEMPLATE_ID = '382e4302-551d-4880-bf29-a30acfab2e71.schema';

// flowStatus 值与 Python SDK AICardStatus 一致（cardParamMap 的值必须是字符串）
const AICardStatus = {
  PROCESSING: '1',
  INPUTING: '2',
  FINISHED: '3',
  EXECUTING: '4',
  FAILED: '5',
} as const;

interface AICardInstance {
  cardInstanceId: string;
  accessToken: string;
  inputingStarted: boolean;
}

// 创建 AI Card 实例
async function createAICard(
  config: any,
  data: any,
  log?: any,
): Promise<AICardInstance | null> {
  try {
    const token = await getAccessToken(config);
    const cardInstanceId = `card_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;

    log?.info?.(`[DingTalk][AICard] 开始创建卡片 outTrackId=${cardInstanceId}`);
    log?.info?.(`[DingTalk][AICard] conversationType=${data.conversationType}, conversationId=${data.conversationId}, senderStaffId=${data.senderStaffId}, senderId=${data.senderId}`);

    // 1. 创建卡片实例（Python SDK 传空 cardParamMap，不预设 flowStatus）
    const createBody = {
      cardTemplateId: AI_CARD_TEMPLATE_ID,
      outTrackId: cardInstanceId,
      cardData: {
        cardParamMap: {},
      },
      callbackType: 'STREAM',
      imGroupOpenSpaceModel: { supportForward: true },
      imRobotOpenSpaceModel: { supportForward: true },
    };

    log?.info?.(`[DingTalk][AICard] POST /v1.0/card/instances body=${JSON.stringify(createBody)}`);
    const createResp = await axios.post(`${DINGTALK_API}/v1.0/card/instances`, createBody, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    });
    log?.info?.(`[DingTalk][AICard] 创建卡片响应: status=${createResp.status} data=${JSON.stringify(createResp.data)}`);

    // 2. 投放卡片
    const isGroup = data.conversationType === '2';
    const deliverBody: any = {
      outTrackId: cardInstanceId,
      userIdType: 1,
    };

    if (isGroup) {
      deliverBody.openSpaceId = `dtv1.card//IM_GROUP.${data.conversationId}`;
      deliverBody.imGroupOpenDeliverModel = {
        robotCode: config.clientId,
      };
    } else {
      const userId = data.senderStaffId || data.senderId;
      deliverBody.openSpaceId = `dtv1.card//IM_ROBOT.${userId}`;
      deliverBody.imRobotOpenDeliverModel = { spaceType: 'IM_ROBOT' };
    }

    log?.info?.(`[DingTalk][AICard] POST /v1.0/card/instances/deliver body=${JSON.stringify(deliverBody)}`);
    const deliverResp = await axios.post(`${DINGTALK_API}/v1.0/card/instances/deliver`, deliverBody, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    });
    log?.info?.(`[DingTalk][AICard] 投放卡片响应: status=${deliverResp.status} data=${JSON.stringify(deliverResp.data)}`);

    return { cardInstanceId, accessToken: token, inputingStarted: false };
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard] 创建卡片失败: ${err.message}`);
    if (err.response) {
      log?.error?.(`[DingTalk][AICard] 错误响应: status=${err.response.status} data=${JSON.stringify(err.response.data)}`);
    }
    return null;
  }
}

// 流式更新 AI Card 内容
async function streamAICard(
  card: AICardInstance,
  content: string,
  finished: boolean = false,
  log?: any,
): Promise<void> {
  // 首次 streaming 前，先切换到 INPUTING 状态（与 Python SDK get_card_data(INPUTING) 一致）
  if (!card.inputingStarted) {
    const statusBody = {
      outTrackId: card.cardInstanceId,
      cardData: {
        cardParamMap: {
          flowStatus: AICardStatus.INPUTING,
          msgContent: '',
          staticMsgContent: '',
          sys_full_json_obj: JSON.stringify({
            order: ['msgContent'],  // 只声明实际使用的字段，避免部分客户端显示空占位
          }),
        },
      },
    };
    log?.info?.(`[DingTalk][AICard] PUT /v1.0/card/instances (INPUTING) outTrackId=${card.cardInstanceId}`);
    try {
      const statusResp = await axios.put(`${DINGTALK_API}/v1.0/card/instances`, statusBody, {
        headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
      });
      log?.info?.(`[DingTalk][AICard] INPUTING 响应: status=${statusResp.status} data=${JSON.stringify(statusResp.data)}`);
    } catch (err: any) {
      log?.error?.(`[DingTalk][AICard] INPUTING 切换失败: ${err.message}, resp=${JSON.stringify(err.response?.data)}`);
      throw err;
    }
    card.inputingStarted = true;
  }

  // 调用 streaming API 更新内容
  const body = {
    outTrackId: card.cardInstanceId,
    guid: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    key: 'msgContent',
    content: content,
    isFull: true,  // 全量替换
    isFinalize: finished,
    isError: false,
  };

  log?.info?.(`[DingTalk][AICard] PUT /v1.0/card/streaming contentLen=${content.length} isFinalize=${finished} guid=${body.guid}`);
  try {
    const streamResp = await axios.put(`${DINGTALK_API}/v1.0/card/streaming`, body, {
      headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
    });
    log?.info?.(`[DingTalk][AICard] streaming 响应: status=${streamResp.status}`);
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard] streaming 更新失败: ${err.message}, resp=${JSON.stringify(err.response?.data)}`);
    throw err;
  }
}

// 完成 AI Card：先 streaming isFinalize 关闭流式通道，再 put_card_data 更新 FINISHED 状态
async function finishAICard(
  card: AICardInstance,
  content: string,
  log?: any,
): Promise<void> {
  log?.info?.(`[DingTalk][AICard] 开始 finish，最终内容长度=${content.length}`);

  // 1. 先用最终内容关闭流式通道（isFinalize=true），确保卡片显示替换后的内容
  await streamAICard(card, content, true, log);

  // 2. 更新卡片状态为 FINISHED
  const body = {
    outTrackId: card.cardInstanceId,
    cardData: {
      cardParamMap: {
        flowStatus: AICardStatus.FINISHED,
        msgContent: content,
        staticMsgContent: '',
        sys_full_json_obj: JSON.stringify({
          order: ['msgContent'],  // 只声明实际使用的字段，避免部分客户端显示空占位
        }),
      },
    },
  };

  log?.info?.(`[DingTalk][AICard] PUT /v1.0/card/instances (FINISHED) outTrackId=${card.cardInstanceId}`);
  try {
    const finishResp = await axios.put(`${DINGTALK_API}/v1.0/card/instances`, body, {
      headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
    });
    log?.info?.(`[DingTalk][AICard] FINISHED 响应: status=${finishResp.status} data=${JSON.stringify(finishResp.data)}`);
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard] FINISHED 更新失败: ${err.message}, resp=${JSON.stringify(err.response?.data)}`);
  }
}

// ============ Gateway SSE Streaming ============

interface GatewayOptions {
  userContent: string;
  systemPrompts: string[];
  sessionKey: string;
  gatewayAuth?: string;  // token 或 password，都用 Bearer 格式
  log?: any;
}

async function* streamFromGateway(options: GatewayOptions): AsyncGenerator<string, void, unknown> {
  const { userContent, systemPrompts, sessionKey, gatewayAuth, log } = options;
  const rt = getRuntime();
  const gatewayUrl = `http://127.0.0.1:${rt.gateway?.port || 18789}/v1/chat/completions`;

  const messages: any[] = [];
  for (const prompt of systemPrompts) {
    messages.push({ role: 'system', content: prompt });
  }
  messages.push({ role: 'user', content: userContent });

  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (gatewayAuth) {
    headers['Authorization'] = `Bearer ${gatewayAuth}`;
  }

  log?.info?.(`[DingTalk][Gateway] POST ${gatewayUrl}, session=${sessionKey}, messages=${messages.length}`);

  const response = await fetch(gatewayUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      model: 'default',
      messages,
      stream: true,
      user: sessionKey,  // 用于 session 持久化
    }),
  });

  log?.info?.(`[DingTalk][Gateway] 响应 status=${response.status}, ok=${response.ok}, hasBody=${!!response.body}`);

  if (!response.ok || !response.body) {
    const errText = response.body ? await response.text() : '(no body)';
    log?.error?.(`[DingTalk][Gateway] 错误响应: ${errText}`);
    throw new Error(`Gateway error: ${response.status} - ${errText}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (!line.startsWith('data: ')) continue;
      const data = line.slice(6).trim();
      if (data === '[DONE]') return;

      try {
        const chunk = JSON.parse(data);
        const content = chunk.choices?.[0]?.delta?.content;
        if (content) yield content;
      } catch {}
    }
  }
}

// ============ 消息处理 ============

function extractMessageContent(data: any): { text: string; messageType: string } {
  const msgtype = data.msgtype || 'text';
  switch (msgtype) {
    case 'text':
      return { text: data.text?.content?.trim() || '', messageType: 'text' };
    case 'richText': {
      const parts = data.content?.richText || [];
      const text = parts.filter((p: any) => p.type === 'text').map((p: any) => p.text).join('');
      return { text: text || '[富文本消息]', messageType: 'richText' };
    }
    case 'picture':
      return { text: '[图片]', messageType: 'picture' };
    case 'audio':
      return { text: data.content?.recognition || '[语音消息]', messageType: 'audio' };
    case 'video':
      return { text: '[视频]', messageType: 'video' };
    case 'file':
      return { text: `[文件: ${data.content?.fileName || '文件'}]`, messageType: 'file' };
    default:
      return { text: data.text?.content?.trim() || `[${msgtype}消息]`, messageType: msgtype };
  }
}

// 发送 Markdown 消息
async function sendMarkdownMessage(
  config: any,
  sessionWebhook: string,
  title: string,
  markdown: string,
  options: any = {},
): Promise<any> {
  const token = await getAccessToken(config);
  let text = markdown;
  if (options.atUserId) text = `${text} @${options.atUserId}`;

  const body: any = {
    msgtype: 'markdown',
    markdown: { title: title || 'Moltbot', text },
  };
  if (options.atUserId) body.at = { atUserIds: [options.atUserId], isAtAll: false };

  return (await axios.post(sessionWebhook, body, {
    headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
  })).data;
}

// 发送文本消息
async function sendTextMessage(
  config: any,
  sessionWebhook: string,
  text: string,
  options: any = {},
): Promise<any> {
  const token = await getAccessToken(config);
  const body: any = { msgtype: 'text', text: { content: text } };
  if (options.atUserId) body.at = { atUserIds: [options.atUserId], isAtAll: false };

  return (await axios.post(sessionWebhook, body, {
    headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
  })).data;
}

// 主动发送消息到指定用户（用于 outbound）
async function sendProactiveMessage(
  config: any,
  userId: string,
  text: string,
  log?: any,
): Promise<{ ok: boolean; messageId?: string; error?: string }> {
  try {
    const token = await getAccessToken(config);

    // 使用钉钉机器人批量发送消息 API
    const body = {
      robotCode: config.clientId,
      userIds: [userId],
      msgKey: 'sampleText',
      msgParam: JSON.stringify({ content: text }),
    };

    log?.info?.(`[DingTalk][Outbound] 发送消息到 ${userId}: "${text.slice(0, 50)}..."`);

    const response = await axios.post(
      'https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend',
      body,
      {
        headers: {
          'x-acs-dingtalk-access-token': token,
          'Content-Type': 'application/json',
        },
      },
    );

    log?.info?.(`[DingTalk][Outbound] 发送成功: ${JSON.stringify(response.data)}`);

    return {
      ok: true,
      messageId: response.data?.processQueryKey || 'unknown',
    };
  } catch (err: any) {
    log?.error?.(`[DingTalk][Outbound] 发送失败: ${err.message}`);
    if (err.response) {
      log?.error?.(`[DingTalk][Outbound] 错误响应: ${JSON.stringify(err.response.data)}`);
    }
    return {
      ok: false,
      error: err.message,
    };
  }
}

// 发送群消息（文本模式，降级方案）
async function sendGroupMessage(
  config: any,
  conversationId: string,
  text: string,
  log?: any,
): Promise<{ ok: boolean; messageId?: string; error?: string }> {
  try {
    const token = await getAccessToken(config);

    const body = {
      robotCode: config.clientId,
      openConversationId: conversationId,
      msgKey: 'sampleText',
      msgParam: JSON.stringify({ content: text }),
    };

    log?.info?.(`[DingTalk][Outbound][Group] 发送文本消息到群 ${conversationId}`);

    const response = await axios.post(
      'https://api.dingtalk.com/v1.0/robot/groupMessages/send',
      body,
      {
        headers: {
          'x-acs-dingtalk-access-token': token,
          'Content-Type': 'application/json',
        },
      },
    );

    log?.info?.(`[DingTalk][Outbound][Group] 发送成功: ${JSON.stringify(response.data)}`);

    return {
      ok: true,
      messageId: response.data?.processQueryKey || 'unknown',
    };
  } catch (err: any) {
    log?.error?.(`[DingTalk][Outbound][Group] 发送失败: ${err.message}`);
    if (err.response) {
      log?.error?.(`[DingTalk][Outbound][Group] 错误响应: ${JSON.stringify(err.response.data)}`);
    }
    return {
      ok: false,
      error: err.message,
    };
  }
}

// 创建主动投放的 AI Card
async function createProactiveAICard(
  config: any,
  target: string,
  isGroup: boolean,
  log?: any,
): Promise<AICardInstance | null> {
  try {
    const token = await getAccessToken(config);
    const cardInstanceId = `card_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;

    log?.info?.(`[DingTalk][AICard][Proactive] 创建卡片 outTrackId=${cardInstanceId}, target=${target}, isGroup=${isGroup}`);

    // 1. 创建卡片实例
    const createBody = {
      cardTemplateId: AI_CARD_TEMPLATE_ID,
      outTrackId: cardInstanceId,
      cardData: {
        cardParamMap: {},
      },
      callbackType: 'STREAM',
      imGroupOpenSpaceModel: { supportForward: true },
      imRobotOpenSpaceModel: { supportForward: true },
    };

    log?.info?.(`[DingTalk][AICard][Proactive] POST /v1.0/card/instances`);
    const createResp = await axios.post(`${DINGTALK_API}/v1.0/card/instances`, createBody, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    });
    log?.info?.(`[DingTalk][AICard][Proactive] 创建响应: ${JSON.stringify(createResp.data)}`);

    // 2. 投放卡片（根据目标类型构造不同的 deliverBody）
    const deliverBody: any = {
      outTrackId: cardInstanceId,
      userIdType: 1,
    };

    if (isGroup) {
      deliverBody.openSpaceId = `dtv1.card//IM_GROUP.${target}`;
      deliverBody.imGroupOpenDeliverModel = {
        robotCode: config.clientId,
      };
    } else {
      deliverBody.openSpaceId = `dtv1.card//IM_ROBOT.${target}`;
      deliverBody.imRobotOpenDeliverModel = { spaceType: 'IM_ROBOT' };
    }

    log?.info?.(`[DingTalk][AICard][Proactive] POST /v1.0/card/instances/deliver`);
    const deliverResp = await axios.post(`${DINGTALK_API}/v1.0/card/instances/deliver`, deliverBody, {
      headers: { 'x-acs-dingtalk-access-token': token, 'Content-Type': 'application/json' },
    });
    log?.info?.(`[DingTalk][AICard][Proactive] 投放响应: ${JSON.stringify(deliverResp.data)}`);

    return { cardInstanceId, accessToken: token, inputingStarted: false };
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard][Proactive] 创建失败: ${err.message}`);
    if (err.response) {
      log?.error?.(`[DingTalk][AICard][Proactive] 错误响应: ${JSON.stringify(err.response.data)}`);
    }
    return null;
  }
}

// 发送主动 AI Card 消息（支持流式）
async function sendProactiveAICardMessage(
  config: any,
  target: string,
  isGroup: boolean,
  text: string,
  log?: any,
): Promise<{ ok: boolean; messageId?: string; error?: string }> {
  try {
    // 1. 创建 AI Card
    const card = await createProactiveAICard(config, target, isGroup, log);
    if (!card) {
      return { ok: false, error: 'Failed to create AI Card' };
    }

    log?.info?.(`[DingTalk][AICard][Proactive] 开始流式输出，内容长度=${text.length}`);

    // 2. 流式输出内容（模拟打字机效果）
    const chunkSize = 50; // 每次输出 50 个字符
    let accumulated = '';

    for (let i = 0; i < text.length; i += chunkSize) {
      accumulated = text.slice(0, i + chunkSize);
      await streamAICard(card, accumulated, false, log);
      await new Promise(resolve => setTimeout(resolve, 300)); // 300ms 间隔
    }

    // 3. 完成输出
    await finishAICard(card, text, log);

    log?.info?.(`[DingTalk][AICard][Proactive] 发送完成，cardInstanceId=${card.cardInstanceId}`);

    return {
      ok: true,
      messageId: card.cardInstanceId,
    };
  } catch (err: any) {
    log?.error?.(`[DingTalk][AICard][Proactive] 发送失败: ${err.message}`);
    return {
      ok: false,
      error: err.message,
    };
  }
}

// 智能选择 text / markdown
async function sendMessage(
  config: any,
  sessionWebhook: string,
  text: string,
  options: any = {},
): Promise<any> {
  const hasMarkdown = /^[#*>-]|[*_`#\[\]]/.test(text) || text.includes('\n');
  const useMarkdown = options.useMarkdown !== false && (options.useMarkdown || hasMarkdown);

  if (useMarkdown) {
    const title = options.title
      || text.split('\n')[0].replace(/^[#*\s\->]+/, '').slice(0, 20)
      || 'Moltbot';
    return sendMarkdownMessage(config, sessionWebhook, title, text, options);
  }
  return sendTextMessage(config, sessionWebhook, text, options);
}

// ============ 核心消息处理 (AI Card Streaming) ============

async function handleDingTalkMessage(params: {
  cfg: ClawdbotConfig;
  accountId: string;
  data: any;
  sessionWebhook: string;
  log?: any;
  dingtalkConfig: any;
}): Promise<void> {
  const { cfg, accountId, data, sessionWebhook, log, dingtalkConfig } = params;
  const rt = getRuntime();

  const content = extractMessageContent(data);
  if (!content.text) return;

  const isDirect = data.conversationType === '1';
  const senderId = data.senderStaffId || data.senderId;
  const senderName = data.senderNick || 'Unknown';

  log?.info?.(`[DingTalk] 收到消息: from=${senderName} text="${content.text.slice(0, 50)}..."`);

  // ===== Session 管理 =====
  const sessionTimeout = dingtalkConfig.sessionTimeout ?? 1800000; // 默认 30 分钟
  const forceNewSession = isNewSessionCommand(content.text);

  // 如果是新会话命令，直接回复确认消息
  if (forceNewSession) {
    const { sessionKey } = getSessionKey(senderId, true, sessionTimeout, log);
    await sendMessage(dingtalkConfig, sessionWebhook, '✨ 已开启新会话，之前的对话已清空。', {
      atUserId: !isDirect ? senderId : null,
    });
    log?.info?.(`[DingTalk] 用户请求新会话: ${senderId}, newKey=${sessionKey}`);
    return;
  }

  // 获取或创建 session
  const { sessionKey, isNew } = getSessionKey(senderId, false, sessionTimeout, log);
  log?.info?.(`[DingTalk][Session] key=${sessionKey}, isNew=${isNew}`);

  // Gateway 认证：优先使用 token，其次 password
  const gatewayAuth = dingtalkConfig.gatewayToken || dingtalkConfig.gatewayPassword || '';

  // 构建 system prompts & 获取 oapi token（用于图片后处理）
  const systemPrompts: string[] = [];
  let oapiToken: string | null = null;

  if (dingtalkConfig.enableMediaUpload !== false) {
    // 添加图片使用提示（告诉 LLM 直接输出本地路径）
    systemPrompts.push(buildMediaSystemPrompt());
    // 获取 token 用于后处理上传
    oapiToken = await getOapiAccessToken(dingtalkConfig);
    log?.info?.(`[DingTalk][Media] oapiToken 获取${oapiToken ? '成功' : '失败'}`);
  } else {
    log?.info?.(`[DingTalk][Media] enableMediaUpload=false，跳过`);
  }

  // 自定义 system prompt
  if (dingtalkConfig.systemPrompt) {
    systemPrompts.push(dingtalkConfig.systemPrompt);
  }

  // 尝试创建 AI Card
  const card = await createAICard(dingtalkConfig, data, log);

  if (card) {
    // ===== AI Card 流式模式 =====
    log?.info?.(`[DingTalk] AI Card 创建成功: ${card.cardInstanceId}`);

    let accumulated = '';
    let lastUpdateTime = 0;
    const updateInterval = 300; // 最小更新间隔 ms
    let chunkCount = 0;

    try {
      log?.info?.(`[DingTalk] 开始请求 Gateway 流式接口...`);
      for await (const chunk of streamFromGateway({
        userContent: content.text,
        systemPrompts,
        sessionKey,
        gatewayAuth,
        log,
      })) {
        accumulated += chunk;
        chunkCount++;

        if (chunkCount <= 3) {
          log?.info?.(`[DingTalk] Gateway chunk #${chunkCount}: "${chunk.slice(0, 50)}..." (accumulated=${accumulated.length})`);
        }

        // 节流更新，避免过于频繁
        const now = Date.now();
        if (now - lastUpdateTime >= updateInterval) {
          await streamAICard(card, accumulated, false, log);
          lastUpdateTime = now;
        }
      }

      log?.info?.(`[DingTalk] Gateway 流完成，共 ${chunkCount} chunks, ${accumulated.length} 字符`);

      // 后处理：上传本地图片到钉钉，替换 file:// 路径为 media_id
      log?.info?.(`[DingTalk][Media] 开始后处理，oapiToken=${oapiToken ? '有' : '无'}，内容片段="${accumulated.slice(0, 200)}..."`);
      accumulated = await processLocalImages(accumulated, oapiToken, log);

      // 完成
      await finishAICard(card, accumulated, log);
      log?.info?.(`[DingTalk] 流式响应完成，共 ${accumulated.length} 字符`);

    } catch (err: any) {
      log?.error?.(`[DingTalk] Gateway 调用失败: ${err.message}`);
      log?.error?.(`[DingTalk] 错误详情: ${err.stack}`);
      accumulated += `\n\n⚠️ 响应中断: ${err.message}`;
      try {
        await finishAICard(card, accumulated, log);
      } catch (finishErr: any) {
        log?.error?.(`[DingTalk] 错误恢复 finish 也失败: ${finishErr.message}`);
      }
    }

  } else {
    // ===== 降级：普通消息模式 =====
    log?.warn?.(`[DingTalk] AI Card 创建失败，降级为普通消息`);

    let fullResponse = '';
    try {
      for await (const chunk of streamFromGateway({
        userContent: content.text,
        systemPrompts,
        sessionKey,
        gatewayAuth,
        log,
      })) {
        fullResponse += chunk;
      }

      await sendMessage(dingtalkConfig, sessionWebhook, fullResponse || '（无响应）', {
        atUserId: !isDirect ? senderId : null,
        useMarkdown: true,
      });
      log?.info?.(`[DingTalk] 普通消息回复完成，共 ${fullResponse.length} 字符`);

    } catch (err: any) {
      log?.error?.(`[DingTalk] Gateway 调用失败: ${err.message}`);
      await sendMessage(dingtalkConfig, sessionWebhook, `抱歉，处理请求时出错: ${err.message}`, {
        atUserId: !isDirect ? senderId : null,
      });
    }
  }
}

// ============ 插件定义 ============

const meta = {
  id: 'dingtalk-connector',
  label: 'DingTalk',
  selectionLabel: 'DingTalk (钉钉)',
  docsPath: '/channels/dingtalk-connector',
  docsLabel: 'dingtalk-connector',
  blurb: '钉钉企业内部机器人，使用 Stream 模式，无需公网 IP，支持 AI Card 流式响应。',
  order: 70,
  aliases: ['dd', 'ding'],
};

const dingtalkPlugin = {
  id: 'dingtalk-connector',
  meta,
  capabilities: {
    chatTypes: ['direct', 'group'],
    reactions: false,
    threads: false,
    media: true,
    nativeCommands: false,
    blockStreaming: false,
  },
  reload: { configPrefixes: ['channels.dingtalk-connector'] },
  configSchema: {
    schema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        enabled: { type: 'boolean', default: true },
        clientId: { type: 'string', description: 'DingTalk App Key (Client ID)' },
        clientSecret: { type: 'string', description: 'DingTalk App Secret (Client Secret)' },
        enableMediaUpload: { type: 'boolean', default: true, description: 'Enable media upload prompt injection' },
        systemPrompt: { type: 'string', default: '', description: 'Custom system prompt' },
        dmPolicy: { type: 'string', enum: ['open', 'pairing', 'allowlist'], default: 'open' },
        allowFrom: { type: 'array', items: { type: 'string' }, description: 'Allowed sender IDs' },
        dms: {
          type: 'object',
          additionalProperties: {
            type: 'object',
            properties: {
              enabled: { type: 'boolean', default: true },
              systemPrompt: { type: 'string' },
            }
          },
          description: 'Per-user DM configurations'
        },
        groupPolicy: { type: 'string', enum: ['open', 'allowlist'], default: 'open' },
        groupAllowFrom: { type: 'array', items: { type: 'string' }, description: 'Allowed group conversationIds' },
        groups: {
          type: 'object',
          additionalProperties: {
            type: 'object',
            properties: {
              enabled: { type: 'boolean', default: true },
              requireMention: { type: 'boolean', default: true },
              systemPrompt: { type: 'string' },
            }
          },
          description: 'Group configurations by conversationId'
        },
        gatewayToken: { type: 'string', default: '', description: 'Gateway auth token (Bearer)' },
        gatewayPassword: { type: 'string', default: '', description: 'Gateway auth password (alternative to token)' },
        sessionTimeout: { type: 'number', default: 1800000, description: 'Session timeout in ms (default 30min)' },
        debug: { type: 'boolean', default: false },
      },
      required: ['clientId', 'clientSecret'],
    },
    uiHints: {
      enabled: { label: 'Enable DingTalk' },
      clientId: { label: 'App Key', sensitive: false },
      clientSecret: { label: 'App Secret', sensitive: true },
      dmPolicy: { label: 'DM Policy' },
      groupPolicy: { label: 'Group Policy' },
    },
  },
  config: {
    listAccountIds: (cfg: ClawdbotConfig) => {
      const config = getConfig(cfg);
      return config.accounts
        ? Object.keys(config.accounts)
        : (isConfigured(cfg) ? ['default'] : []);
    },
    resolveAccount: (cfg: ClawdbotConfig, accountId?: string) => {
      const config = getConfig(cfg);
      const id = accountId || 'default';
      if (config.accounts?.[id]) {
        return { accountId: id, config: config.accounts[id], enabled: config.accounts[id].enabled !== false };
      }
      return { accountId: 'default', config, enabled: config.enabled !== false };
    },
    defaultAccountId: () => 'default',
    isConfigured: (account: any) => Boolean(account.config?.clientId && account.config?.clientSecret),
    describeAccount: (account: any) => ({
      accountId: account.accountId,
      name: account.config?.name || 'DingTalk',
      enabled: account.enabled,
      configured: Boolean(account.config?.clientId),
    }),
  },
  security: {
    resolveDmPolicy: ({ account }: any) => ({
      policy: account.config?.dmPolicy || 'open',
      allowFrom: account.config?.allowFrom || [],
      policyPath: 'channels.dingtalk-connector.dmPolicy',
      allowFromPath: 'channels.dingtalk-connector.allowFrom',
      approveHint: '使用 /allow dingtalk-connector:<userId> 批准用户',
      normalizeEntry: (raw: string) => raw.replace(/^(dingtalk-connector|dingtalk|dd|ding):/i, ''),
    }),
  },
  groups: {
    resolveRequireMention: ({ cfg }: any) => getConfig(cfg).groupPolicy !== 'open',
  },
  messaging: {
    normalizeTarget: ({ target }: any) => {
      if (!target) return null;

      // 去掉前缀
      let id = target.replace(/^(dingtalk-connector|dingtalk|dd|ding):/i, '');

      // 支持显式指定类型：user:xxx 或 group:xxx
      let kind: 'user' | 'group';
      if (id.startsWith('user:')) {
        kind = 'user';
        id = id.slice(5); // 去掉 "user:"
      } else if (id.startsWith('group:')) {
        kind = 'group';
        id = id.slice(6); // 去掉 "group:"
      } else {
        // 自动检测：群ID格式为 cidxxxx==（base64编码）
        kind = /^cid[A-Za-z0-9+/]+=*$/.test(id) ? 'group' : 'user';
      }

      return {
        targetId: id,
        kind,
      };
    },
    targetResolver: {
      looksLikeId: (id: string) => {
        // 群ID：cidxxxx== 格式
        if (/^cid[A-Za-z0-9+/]+=*$/.test(id)) return true;
        // 用户ID：字母数字组合
        if (/^[\w-]+$/.test(id)) return true;
        return false;
      },
      hint: '<user:userId | group:conversationId | userId | conversationId>',
    },
  },
  directory: {
    listPeers: async ({ account }: any) => {
      const allowFrom = account.config?.allowFrom || [];
      const dms = account.config?.dms || {};

      const peers: Array<{ kind: 'user'; id: string }> = [];

      // 从 allowFrom 添加
      for (const id of allowFrom) {
        peers.push({ kind: 'user', id });
      }

      // 从 dms 添加
      for (const id of Object.keys(dms)) {
        if (!peers.some(p => p.id === id)) {
          peers.push({ kind: 'user', id });
        }
      }

      return peers;
    },

    listGroups: async ({ account }: any) => {
      const groupAllowFrom = account.config?.groupAllowFrom || [];
      const groups = account.config?.groups || {};

      const result: Array<{ kind: 'group'; id: string }> = [];

      // 从 groupAllowFrom 添加
      for (const id of groupAllowFrom) {
        result.push({ kind: 'group', id });
      }

      // 从 groups 添加
      for (const id of Object.keys(groups)) {
        if (!result.some(g => g.id === id)) {
          result.push({ kind: 'group', id });
        }
      }

      return result;
    },
  },
  outbound: {
    deliveryMode: 'direct' as const,
    sendText: async ({ to, text, accountId, cfg }: any) => {
      const rt = getRuntime();
      const account = dingtalkPlugin.config.resolveAccount(cfg, accountId);
      const config = account.config;

      if (!config.clientId || !config.clientSecret) {
        return { ok: false as const, error: 'DingTalk not configured' };
      }

      // 解析目标类型
      const normalized = dingtalkPlugin.messaging.normalizeTarget({ target: to });
      if (!normalized) {
        return { ok: false as const, error: 'Invalid target' };
      }

      const { targetId, kind } = normalized;
      const isGroup = kind === 'group';

      rt.logger?.info?.(`[DingTalk][Outbound] 发送消息到 ${isGroup ? '群' : '个人'}: ${targetId}`);

      // 策略：优先使用 AI Card（流式体验更好），失败时降级到文本消息
      let result = await sendProactiveAICardMessage(config, targetId, isGroup, text, rt.logger);

      if (!result.ok) {
        rt.logger?.warn?.(`[DingTalk][Outbound] AI Card 发送失败，降级到文本消息`);

        // 降级到文本消息
        if (isGroup) {
          result = await sendGroupMessage(config, targetId, text, rt.logger);
        } else {
          result = await sendProactiveMessage(config, targetId, text, rt.logger);
        }
      }

      if (result.ok) {
        return {
          ok: true as const,
          channel: 'dingtalk-connector' as const,
          messageId: result.messageId || 'unknown',
        };
      }

      return { ok: false as const, error: result.error || 'Unknown error' };
    },
  },
  gateway: {
    startAccount: async (ctx: any) => {
      const { account, cfg, abortSignal } = ctx;
      const config = account.config;

      if (!config.clientId || !config.clientSecret) {
        throw new Error('DingTalk clientId and clientSecret are required');
      }

      ctx.log?.info(`[${account.accountId}] 启动钉钉 Stream 客户端...`);

      const client = new DWClient({
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        debug: config.debug || false,
      });

      client.registerCallbackListener(TOPIC_ROBOT, async (res: any) => {
        try {
          const messageId = res.headers?.messageId;
          ctx.log?.info?.(`[DingTalk] 收到 Stream 回调, messageId=${messageId}, headers=${JSON.stringify(res.headers)}`);
          ctx.log?.info?.(`[DingTalk] 原始 data: ${typeof res.data === 'string' ? res.data.slice(0, 500) : JSON.stringify(res.data).slice(0, 500)}`);
          const data = JSON.parse(res.data);

          await handleDingTalkMessage({
            cfg,
            accountId: account.accountId,
            data,
            sessionWebhook: data.sessionWebhook,
            log: ctx.log,
            dingtalkConfig: config,
          });

          if (messageId) client.socketCallBackResponse(messageId, { success: true });
        } catch (error: any) {
          ctx.log?.error?.(`[DingTalk] 处理消息异常: ${error.message}`);
          const messageId = res.headers?.messageId;
          if (messageId) client.socketCallBackResponse(messageId, { success: false });
        }
      });

      await client.connect();
      ctx.log?.info(`[${account.accountId}] 钉钉 Stream 客户端已连接`);

      const rt = getRuntime();
      rt.channel.activity.record('dingtalk-connector', account.accountId, 'start');

      let stopped = false;
      if (abortSignal) {
        abortSignal.addEventListener('abort', () => {
          if (stopped) return;
          stopped = true;
          ctx.log?.info(`[${account.accountId}] 停止钉钉 Stream 客户端...`);
          rt.channel.activity.record('dingtalk-connector', account.accountId, 'stop');
        });
      }

      return {
        stop: () => {
          if (stopped) return;
          stopped = true;
          ctx.log?.info(`[${account.accountId}] 钉钉 Channel 已停止`);
          rt.channel.activity.record('dingtalk-connector', account.accountId, 'stop');
        },
      };
    },
  },
  status: {
    defaultRuntime: { accountId: 'default', running: false, lastStartAt: null, lastStopAt: null, lastError: null },
    probe: async ({ cfg }: any) => {
      if (!isConfigured(cfg)) return { ok: false, error: 'Not configured' };
      try {
        const config = getConfig(cfg);
        await getAccessToken(config);
        return { ok: true, details: { clientId: config.clientId } };
      } catch (error: any) {
        return { ok: false, error: error.message };
      }
    },
    buildChannelSummary: ({ snapshot }: any) => ({
      configured: snapshot?.configured ?? false,
      running: snapshot?.running ?? false,
      lastStartAt: snapshot?.lastStartAt ?? null,
      lastStopAt: snapshot?.lastStopAt ?? null,
      lastError: snapshot?.lastError ?? null,
    }),
  },
};

// ============ 插件注册 ============

const plugin = {
  id: 'dingtalk-connector',
  name: 'DingTalk Channel',
  description: 'DingTalk (钉钉) messaging channel via Stream mode with AI Card streaming',
  configSchema: {
    type: 'object',
    additionalProperties: true,
    properties: { enabled: { type: 'boolean', default: true } },
  },
  register(api: ClawdbotPluginApi) {
    runtime = api.runtime;
    api.registerChannel({ plugin: dingtalkPlugin });
    api.registerGatewayMethod('dingtalk-connector.status', async ({ respond, cfg }: any) => {
      const result = await dingtalkPlugin.status.probe({ cfg });
      respond(true, result);
    });
    api.registerGatewayMethod('dingtalk-connector.probe', async ({ respond, cfg }: any) => {
      const result = await dingtalkPlugin.status.probe({ cfg });
      respond(result.ok, result);
    });
    api.logger?.info('[DingTalk] 插件已注册');
  },
};

export default plugin;
export { dingtalkPlugin, sendMessage, sendTextMessage, sendMarkdownMessage };
