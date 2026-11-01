import {
  CompassIcon,
  GraduationCapIcon,
  ImageIcon,
  MicroscopeIcon,
  PenLineIcon,
  ShapesIcon,
  SparklesIcon,
  VideoIcon,
} from "lucide-react";

import { DEFAULT_PRODUCT_NAME } from "@/core/product";

import type { Translations } from "./types";

export function createZhCN(product: string): Translations {
  return {
    people: {
      you: "你",
      youInline: "你",
      aManager: "一位管理员",
      formerNamed: (person) => `${person}（已停用）`,
      former: "一位前同事",
      unknown: "一位同事",
    },
    attention: {
      navCount: (pending, routing) =>
        routing === 0
          ? `${pending} 项等待你的回复`
          : `${pending} 项等待你的回复，${routing} 项等待指定接收人`,
      title: "关注事项",
      pending: "需要你的输入",
      routing: "需要指定接收人",
      answered: "等待核查",
      all: "共享请求",
      empty: "此视图暂无请求。",
      unavailable: "当前部署或权限不支持关注事项。",
      notice: (name) =>
        `回复会保存，但不会启动工作。要让${name ?? "AI 助理或 AI 员工"}继续，请打开任务并点击恢复。`,
      question: "问题",
      reason: "原因",
      expected: "预期回复",
      information: "信息",
      decision: "决定",
      review: "结果审核",
      recipient: "谁来回答",
      defaultRecipient: "留空以使用符合权限的人类主管。",
      create: "请求人类输入",
      purpose: "用途",
      choices: "选项（可选，每行一个）",
      shared: (name) => `所有能查看${name}全部工作的人都能阅读问题和回复。`,
      send: "提交回复",
      supplied: "已提供信息",
      cannot_provide: "无法提供",
      wrong_recipient: "接收人有误",
      text: "回复",
      choice: "选项",
      disposition: "回复类型",
      read: "标记已读",
      readNotice: "阅读不会解决此请求。",
      history: "请求历史",
      responses: "人类回复",
      route: "指定接收人",
      unassign: "清除接收人",
      withdraw: "撤回请求",
      note: "决定或撤回原因",
      decide: "记录正式决定",
      accept: "接受结果陈述",
      ack: "我已审核此确切结果陈述及回复。尚未核查来源的当前内容。",
      refresh: "刷新",
      reset: "放弃过期表单并重新加载",
      stale: "请求已更改。回复前请检查当前问题和接收人。",
      retry: "重试原提交",
      unconfirmed: "尚未确认是否已保存。请重试相同的提交；不会重复保存。",
      receipt: "已保存回复回执",
      saved: "已保存",
      closed: "已关闭",
      back: "上一页",
      next: "下一页",
      openWork: "打开 Work",
      file: "现有文件引用",
      space: "存储空间",
      path: "相对文件路径",
      fileNotice: (name) =>
        `引用指向${name}已经拥有的文件；不会将文件交给${name}。请先通过${name}的文件标签页或与${name}的对话附件交付文件，再在这里引用。`,
      requestLink: "在关注事项中查看请求",
      unchecked: "尚未核查文件当前内容。",
      noAssessment: (name) =>
        `回复已保存。要让${name}使用此回复，请打开任务并点击恢复。`,
      outcome: "已提交的结果陈述",
      state: "请求状态",
      activity: "最近活动",
      recipientMissing: "暂时无人。管理者需要选择一位同事。",
      readState: "已读至版本",
      sources: "来源引用",
      createUnavailable: "新请求需要启用策略且已协调的活动 Work。",
      noPermission: "您没有查看请求的权限。",
      unavailableStill: (name) =>
        `您仍可阅读请求并回复。回复会保留，直到${name ?? "AI 助理和 AI 员工"}能够再次工作。`,
      loadError: "无法加载请求。请检查连接并刷新。",
      emptyPending:
        "目前没有需要您提供的信息。AI 助理或 AI 员工需要您提供文件、事实或决定时，请求会显示在这里。",
      recipientDefault: (supervisor) => `${supervisor}（监督者）`,
      recipientNone: "暂时无人",
      savedAt: (time) => `已于${time}保存。`,
      readCurrent: "您已阅读最新版本。",
      readStale: "阅读后已有更改。",
      sourceUnavailable: "您已无法访问此文件。",
      closedReason: {
        resolved: "已回答并检查",
        superseded: "已由新问题替代",
        withdrawn: "已撤回",
      },
      event: {
        create: "已提问",
        respond: "已回复",
        route: "接收人已更改",
        withdraw: "问题已撤回",
        assess: "回复已检查",
        decide: "已记录决定",
        accept: "已接受结果",
        edit: "已关闭：任务已编辑",
        cancel: "已关闭：任务已撤回",
        reopen: "已关闭：任务已重新开放",
        changes_requested: "已关闭：已要求更改",
        reconcile_mandate: "已按更新后的工作说明检查",
        other: "已更新",
      },
    },
    agentWork: {
      stopPending: (name) => `正在停止${name}的工作，可能需要一分钟。`,
      stopNothing: (name) => `没有需要停止的工作：${name}已经停止处理此任务。`,
      stopFailed: "无法停止工作，请重试。",
      newConversation: "使用新对话",
      reconcileAttempt: "手动关闭此工作",
      containmentReceipt: (name) => `关闭${name}工作环境的更改`,
      containmentNotice: (name) =>
        `仅在工作持续未确认时使用。此操作记录${name}的工作环境已关闭，不会将工作标为完成，也不会重新启动${name}。`,
      responsibilities: "职责",
      currentWork: "本页当前工作",
      needsInput: "本页待关注工作",
      recentOutcomes: "本页近期结果",
      activate: "执行此工作",
      resume: "恢复执行",
      recoverActivation: "再次尝试启动",
      execution: (name) => `${name}的工作`,
      openConversation: "打开任务的对话",
      candidate: "完成报告正在等待确认；尚未提交审核。",
      uncertain: (name) =>
        `${name}的最后一步没有返回结果。正在安全关闭${name}的工作环境，通常不到一分钟。`,
      suggestion: "建议的后续工作",
      delegateSuggestion: "准备委派",
      attemptStatus: {
        starting: "正在启动",
        running: "正在工作",
        stopping: "正在停止…",
        uncertain: "正在确认发生了什么",
        succeeded: "已结束",
        failed: "失败",
        cancelled: "已停止",
        contained: "已中断",
        notStarted: "未启动",
        noResult: "结束但没有结果",
      },
      resetResponse: "为当前记录重置回复",
      replaceUnavailable: "替换引用列表，包括删除不可访问的引用。",
      title: "工作",
      recordsOnly: (name) =>
        `保存任务不会启动工作。点击“执行此工作”或“恢复”后，${name}才开始工作。`,
      visibility: (supervisor) =>
        `要查看和分配任务，您需要“可聊天”和“可查看所有工作”权限。请联系${supervisor}。`,
      policyDisabled: (name) =>
        `${name}的任务功能已关闭。管理者可以在${name}的设置标签页开启。以前保存的任务仍可阅读。`,
      policyEnabled: "启用工作跟踪",
      policyNotice:
        "这会更改定义。使用该定义的 AI 助理和 AI 员工会保留当前设置，直到管理者应用更新后的工作说明。不会自动启动任何工作。",
      reviewRequired: "要求人工审核",
      responsibility: "职责",
      objective: "目标",
      criteria: "成功标准",
      priority: "优先级",
      due: "截止时间（本地时间；不会自动启动）",
      delegate: "委派工作",
      sharedNotice: (name) =>
        `所有能查看${name}全部工作的人都能阅读任务和备注。请只填写您愿意分享的内容。`,
      empty: "暂无工作记录。",
      open: "待处理",
      blocked: "受阻",
      submitted: "待审核",
      completed: "已完成",
      cancelled: "已取消",
      low: "低",
      normal: "普通",
      high: "高",
      urgent: "紧急",
      edit: "编辑任务",
      save: "保存任务",
      cancelEdit: "放弃草稿",
      cancel: "撤回任务",
      reopen: "重新打开工作",
      changes_requested: "要求修改",
      reconcile_mandate: "按更新后的工作说明确认",
      accept: "接受结果陈述",
      reconcileNeeded: (name) =>
        `保存此任务后，${name}的工作说明已更改。管理者需要确认任务仍然适用，${name}才能处理。`,
      note: "备注或回复",
      input: "提供事实信息",
      decide: "记录管理员决定",
      inputNotice: (name) =>
        `此处提供的信息不会解决问题或批准决定，也不会启动工作。要让${name}继续，请点击恢复。`,
      unchecked: "尚未检查当前内容。引用不证明文件存在，也不证明内容已经审核。",
      acceptUnchecked:
        "我接受已记录的结果陈述，并确认文件内容未经检查或不可访问。",
      unavailableSource: "来源不可访问",
      sources: "来源引用",
      space: "存储空间",
      path: "相对文件路径",
      addSource: "添加引用",
      removeSource: "删除引用",
      history: "历史",
      previous: "上一页",
      next: "下一页",
      refresh: "刷新工作",
      retry: "重试同一请求",
      unconfirmed: "请求结果尚未确认。请重试同一请求以获取已记录的结果。",
      error: "工作操作失败。重试前请查看当前记录。",
      progress: "进展",
      nextAction: "下一步",
      lastActivity: "最近活动",
      acceptedBy: "人工审核接受人",
      reportedComplete: (name) => `${name}已报告完成`,
      creator: "委派人",
      human: "人员",
      nonhuman: "AI 员工",
      stop: "停止",
      stopConfirm: (name) =>
        `停止${name}处理此任务吗？${name}已经完成的内容会保留在${name}的文件中，任务保持开放，您之后可以点击恢复。`,
      stopNow: "立即停止",
      keepWorking: "继续工作",
      stopped: (person, time) =>
        `${person}于${time}停止了工作。点击恢复以继续。`,
      stoppedByEdit: (person) =>
        `${person}编辑了任务，因此工作已停止。点击恢复以按新版本继续。`,
      stoppedByWithdraw: (person) => `${person}撤回了任务，因此工作已停止。`,
      stoppedByService: (name) =>
        `服务重启或维护导致工作停止。${name}已经完成的内容保留在${name}的文件中。点击恢复以继续。`,
      stoppedPlain: "已停止。点击恢复以继续。",
      uncertainWaiting: (name) =>
        `${name}的最后一步没有返回结果。待${name}的其他对话结束后，将关闭${name}的工作环境；之后可恢复此任务。`,
      attemptFailed: (name, support) =>
        `${name}因错误停止了此任务。请点击恢复以重试；如果持续发生，请联系${support}。`,
      endedWithoutReport: (name) =>
        `${name}停止了工作，但没有报告结果或在此任务上提问。${name}最后写下的内容在对话中。如果${name}在那里提问，请先在对话中回答或编辑任务，然后点击恢复。`,
      recoverActivationNote: "这不会重复启动工作。",
      giveNeedsChat: (name, supervisor) =>
        `要向${name}分配任务，您还需要“可聊天”权限。请联系${supervisor}。`,
      advanced: "高级",
      latest: "（最新）",
      manualFirst: (name) =>
        `请在${name}的设置标签页暂停并恢复${name}，然后返回此处。`,
      unresolvedAsk: (supervisor) => `请联系${supervisor}检查此情况。`,
      host: "服务",
      attempt: {
        busy: (name, action) =>
          `同时运行的工作过多，因此${name}未能启动。请稍后再次点击${action}。`,
        runLimit: (name, action) =>
          `${name}已在两个对话中工作。请在其中一个结束后点击${action}。`,
        restarting: (name, action) =>
          `${name}的工作环境正在重启。请几分钟后再次点击${action}。`,
        startingElsewhere: (name, action) =>
          `另一个对话正在启动${name}。请稍后再次点击${action}。`,
        startFailed: (name, action, support) =>
          `${name}的工作环境无法启动，未执行任何工作。请点击${action}重试；如果持续发生，请联系${support}。`,
        full: (name, support) =>
          `${name}的文件空间已满，因此${name}无法启动。请在${name}的文件标签页删除文件，或联系${support}增加空间。`,
        fullAsk: (name, supervisor, support) =>
          `${name}的文件空间已满，因此${name}无法启动。请联系${supervisor}删除文件，或联系${support}增加空间。`,
      },
      workLimit: {
        one: "另一个任务正在运行，此处一次只能运行一个。该任务结束后即可启动此任务。",
        many: (count) =>
          `已有${count}个任务正在运行，已达到同时运行的上限。请等其中一个结束后再启动此任务。`,
        which: (agent, objective, time, person) =>
          `${agent}正在处理“${objective}”，由${person}于${time}启动。`,
        starting: (agent) => `${agent}正在启动一个任务。`,
        free: (name) => `现在有空位了。${name}可以开始工作。`,
      },
      event: {
        delegate: "任务已保存",
        activate: "工作已启动",
        progress: "已报告进展",
        outcome: "已报告完成",
        request_input: "已请求输入",
        revise_input: "问题已更改",
        assess_input: "已检查回复",
        suggest: "已建议后续工作",
        derive: "已创建后续任务",
        derived: "由另一任务创建",
        human_response: "已回复",
        request_route: "问题已转给其他人",
        request_withdraw: "问题已撤回",
        input: "已提供信息",
        decide: "已记录决定",
        accept: "已接受结果",
        edit: "任务已编辑",
        cancel: "任务已撤回",
        reopen: "任务已重新开放",
        changes_requested: "已要求更改",
        reconcile_mandate: "已按更新后的工作说明检查",
        reconcile_attempt: "已手动关闭",
        stop: "已请求停止",
        attempt_settled: "工作已结束",
        attempt_uncertain: "工作结束但未确认",
        attempt_contained: "已中断",
        other: "已更新",
      },
      contained: {
        containerAbsent: (name) =>
          `${name}处理此任务时，${name}的工作环境意外停止。`,
        runOrphaned: (name) => `${name}处理此任务时，服务已重启。`,
        runNeverStarted: (name) =>
          `启动${name}的请求未完成，没有更改任何内容。`,
        startFailed: (name, support) =>
          `${name}的工作环境无法启动，未执行任何工作。请点击恢复以重试；如果持续发生，请联系${support}。`,
        outcomeUncertain: (name) =>
          `${name}的最后一步没有返回结果，因此已关闭${name}的工作环境以保护文件。继续前请检查文件。`,
        idle: (name) =>
          `${name}没有完成此任务，且${name}的工作环境在一段时间未使用后关闭。`,
        lifecycle: (name) =>
          `管理者在${name}处理此任务时暂停或停用了${name}，或更改了${name}的权限、监督者或工作说明，关闭了${name}的工作环境。`,
        storageRecovery: (provider, name) =>
          `${provider}在${name}处理此任务时恢复或修复了${name}的文件，关闭了${name}的工作环境。继续前请检查文件。`,
        operator: (name, provider) =>
          `${name}处理此任务时，${provider}关闭了${name}的工作环境以进行维护。`,
        hostReplaced: (name) =>
          `${name}处理此任务时，服务已迁移到新服务器。${name}的文件为上次备份时的版本；继续前请检查文件。`,
        unknown: (name) => `${name}处理此任务的工作已中断。`,
        kept: (name) =>
          `${name}已完成的内容保留在${name}的文件中；没有提交审核。`,
        candidate: (name) =>
          `中断前${name}已写下完成报告，但未提交。请点击恢复，让${name}检查工作并再次报告。`,
      },
    },
    sandboxNetwork: {
      title: "网站被阻止",
      blocked: (name, host, minutes, provider) =>
        `${name} 尝试访问 ${host}，但该网站不在允许列表中。您可以让 ${name} 的全部对话和任务在 ${minutes} 分钟内访问 ${host}，或继续阻止 ${host}。永久列表由 ${provider} 设置。`,
      neverAllowed: "公司内部网络中的地址始终不能被允许。",
      allow: (minutes) => `允许 ${minutes} 分钟`,
      keepBlocked: "继续阻止",
      allowed: (host, minutes, name) =>
        `已允许访问 ${host}，有效期 ${minutes} 分钟。请让 ${name} 重试，或在需要 ${host} 的任务上点击恢复。`,
      kept: (host) => `${host} 仍被阻止。`,
      managersOnly: (name) => `只有能管理 ${name} 的人才能允许网站。`,
      deniedAsk: (name, host, supervisor, support) =>
        `${name} 无法访问 ${host}，该网站不在允许列表中。请联系 ${supervisor} 暂时允许 ${host}，或联系 ${support} 将 ${host} 添加到列表。`,
      denied: (name, host, provider) =>
        `${name} 无法访问 ${host}，该网站不在允许列表中。如需暂时允许，请在您自己的对话中让 ${name} 访问 ${host}，并在询问时允许该网站。然后请 ${name} 重试，或在已结束的任务上点击恢复。永久列表由 ${provider} 设置。`,
    },
    agentInstances: {
      welcome: {
        title: (name) => `${name} 已准备好`,
        body: (name) => `${name} 会在对话之间保留文件和记忆。`,
        ownFiles: (name) =>
          `${name} 在自己的文件中工作，不会操作您的文件或浏览器。要给 ${name} 文件，请在这里附加。`,
        assistantNoMyFiles: (name) => `${name} 无法查看我的文件。`,
        sharedReadable: (name) => `${name} 可以阅读共享文件。`,
        prefilled: "消息框中已准备好第一条消息。只有点击发送后才会发送。",
        firstMessage: (name) =>
          `你好，${name}。请简要介绍你的角色、可以交给你的工作，以及如何交接文件。`,
        tipFiles: (name) => `附加文件以交给 ${name}。`,
        tipWork: (name) => `在工作标签页给 ${name} 分配任务。`,
        tipAccess: "在访问权限标签页添加同事。",
        companyNotice: (name) =>
          `任何可以查看 ${name} 所有工作的人，都可以阅读与这位 AI 员工的对话。`,
      },
      chat: {
        subline: (kind, own, owner, supervisor) =>
          kind === "company"
            ? `AI 员工 · 监督者：${supervisor}`
            : own
              ? "您的 AI 助理"
              : `${owner} 的 AI 助理`,
        alsoWorking: "还在另一个对话中工作",
        colleagues: (person, name) =>
          `这是 ${person} 与 ${name} 的对话。${person} 可以阅读您在此发送的内容。`,
      },
      fileNeedsInspect: (name, supervisor) =>
        `要打开 ${name} 的文件，您需要“可查看所有工作”权限。请联系 ${supervisor}。`,
      busy: (name) =>
        `同时运行的工作过多，因此 ${name} 未能启动。请稍后再次发送消息。`,
      conversationLimit: (name) =>
        `${name} 已在两个对话中工作。请在其中一个结束后再次发送消息。`,
      restarting: (name) =>
        `${name} 的工作环境正在重启，等待另一个对话结束。请几分钟后再次发送消息。`,
      startingElsewhere: (name) =>
        `另一个对话正在启动 ${name}。请稍后再次发送消息。`,
      fullStart: (name, support) =>
        `${name} 的文件空间已满，因此 ${name} 无法启动。请在 ${name} 的文件标签页删除文件，或联系 ${support} 增加空间。`,
      fullStartAsk: (name, supervisor, support) =>
        `${name} 的文件空间已满，因此 ${name} 无法启动。请联系 ${supervisor} 删除文件，或联系 ${support} 增加空间。`,
      interruptedByManager: (name) =>
        `管理者更改了 ${name} 的设置，重启了 ${name} 的工作环境并停止了此回复。${name} 文件中已保存的内容没有丢失。请再次发送消息以继续。`,
      interruptedByRecovery: (provider, name) =>
        `${provider} 恢复或修复了 ${name} 的文件，关闭了 ${name} 的工作环境并停止了此回复。请几分钟后再次发送消息。如果文件已恢复，请先检查文件。`,
      interrupted: (name) =>
        `${name} 的工作环境已重启，停止了此回复。请再次发送消息。`,
      interruptedByService: "服务已重启，停止了此回复。请再次发送消息。",
      bringBack: "重新启用",
      makeEmployee: "用这份职责说明创建 AI 员工",
      makeEmployeeNotice:
        "这会添加一位使用相同职责说明的独立 AI 员工。不会复制文件和记忆；请在新 AI 员工的文件标签页添加所需文件。",
      files: {
        root: (name) => `${name} 的文件`,
        name: "名称",
        size: "大小",
        modified: "更改时间",
        uploadsHint: (name) => `他人提供给 ${name} 的文件`,
        outputsHint: (name) => `${name} 生成的文件`,
        producedBy: (name, date) => `由 ${name} 于 ${date} 生成`,
        add: "添加文件",
        drop: (name) => `将文件拖到此处以提供给 ${name}`,
        choose: "选择文件",
        newFolder: "新建文件夹",
        folderName: "文件夹名称",
        newName: "新名称",
        deleteConfirm: (file) => `删除 ${file}？此操作无法撤销。`,
        needsInspect: (name, supervisor) =>
          `要查看 ${name} 的文件，您需要“可查看所有工作”权限。请联系 ${supervisor}。`,
        readOnlyHint: (name) =>
          `要提供文件给 ${name}，请在与 ${name} 的对话中附加文件。`,
        emptyRoot: (name) => `此处尚无内容。拖放文件以提供给 ${name}。`,
        emptyRootReader: "此处尚无内容。",
        uploading: (percent) => `正在上传… ${percent}%`,
        saved: (name) => `已保存在 ${name} 的文件中`,
        savedAs: (file) => `已另存为 ${file}`,
        refused: (file) => `无法保存 ${file}。请重试。`,
        noAccess: (name) => `您已无法提供文件给 ${name}。`,
        full: (name, support) =>
          `${name} 的文件已满。请在 ${name} 的“文件”标签页删除文件，或联系 ${support} 增加容量。`,
        fullAsk: (name, supervisor, support) =>
          `${name} 的文件已满。请联系 ${supervisor} 删除文件，或联系 ${support} 增加容量。`,
        tooLarge: (file, max) => `${file} 大于单个文件的上限 ${max}。`,
        listFailed: (name) => `无法加载 ${name} 的文件。请刷新。`,
        accessNotSaved: (name) =>
          `尚未保存您对 ${name} 文件的访问权限。请管理者在 ${name} 的“访问权限”标签页重新保存您的权限。`,
        listLive: (name) => `${name} 正在工作，文件可能在您查看时更改。`,
        invalidName: "不能使用该名称。",
        reserved: (name) => `该文件夹供 ${name} 自身的工具使用。`,
        nameTaken: "此处已有同名内容。",
        fileChanged: "文件在您打开后已更改。请重新加载，您的文本会保留。",
        folderNotEmpty: "删除文件夹前，请先清空其中的内容。",
        notActive: (name) => `${name} 已暂停或停用，无法更改 ${name} 的文件。`,
        notFound: "该文件或文件夹已不存在。请刷新列表。",
        actionFailed: "操作未成功。请重试。",
      },
      memoryTooLarge:
        "内容过大，无法导入记忆。请粘贴使用“导出记忆”生成的文件内容。",
      memoryInvalid: "这不是记忆导出文件。请粘贴使用“导出记忆”生成的文件内容。",
      create: {
        title: (kind) =>
          kind === "personal" ? "添加 AI 助理" : "添加 AI 员工",
        step: (n) => `第 ${n} 步，共 2 步`,
        namePlaceholder: "例如：报告分析师",
        nameHelp: (kind) =>
          kind === "personal"
            ? "在所有显示这位 AI 助理的地方使用的名称。"
            : "在所有显示这位 AI 员工的地方使用的名称。",
        nameInvalid: "名称须为 1 至 128 个字符。",
        roleHeading: (name) => `${name} 将做什么？`,
        roleHeadingUnnamed: (kind) =>
          kind === "personal"
            ? "这位 AI 助理将做什么？"
            : "这位 AI 员工将做什么？",
        roleBlank: "空白职责说明",
        roleBlankDescription: "自行描述职责。",
        rolesNone: "尚未设置现成的角色。请在下方描述职责。",
        rolesFailed: "无法加载现成的角色。请在下方描述职责，或重试。",
        replaceBrief: "将您编辑的说明替换为这个角色的说明？",
        keepBrief: "保留您的说明",
        useRoleBrief: "使用角色的说明",
        brief: (name) => `${name} 负责什么`,
        briefUnnamed: (kind) =>
          kind === "personal" ? "这位 AI 助理负责什么" : "这位 AI 员工负责什么",
        briefHelp: (name) =>
          `这些内容将成为 ${name} 的常驻指令。您可在 ${name} 的“设置”标签页更改。`,
        briefHelpUnnamed: (kind) =>
          kind === "personal"
            ? "这些内容将成为这位 AI 助理的常驻指令。您可在“设置”标签页更改。"
            : "这些内容将成为这位 AI 员工的常驻指令。您可在“设置”标签页更改。",
        briefPlaceholder:
          "用几句话描述职责：做什么、保留什么，以及何时向人提问。",
        briefRequired: "请简要描述职责。",
        briefTooLong: "职责说明过长。请少于 16,000 个字符。",
        useCustomAgent: "使用您的自定义智能体",
        useRole: "改为描述职责",
        customAgent: "自定义智能体",
        customAgentNone: "您尚无自定义智能体。",
        next: "下一步",
        back: "返回",
        supervisorHeading: (name) => `谁负责 ${name}`,
        supervisorHelp: (name) =>
          `对 ${name} 负责的人。${name} 的问题会优先发送给此人。`,
        settingUp: (name) => `正在设置 ${name}…`,
        slow: "用时比平常更长。请保持此窗口打开。",
        unconfirmed: (name) =>
          `尚未确认是否已添加 ${name}。请点击重试，不会重复添加。`,
        failed: (name) => `无法添加 ${name}。请重试。`,
        briefFailed: "无法保存职责说明。请重试。",
        companyRefused: "只有管理员可以添加 AI 员工。",
        addedNoConversation: (name) =>
          `已添加 ${name}，但无法打开首个对话。请点击“新建对话”重试。`,
      },
      filesBusy: (name) =>
        `${name} 的文件正在更改，暂时无法使用。请过几分钟再试。`,
      assistants: {
        title: "我的助理",
        description:
          "AI 助理属于您：在不同对话间保留职责说明、文件和记忆；只有您能使用，除非您添加同事。AI 助理看不到“我的文件”，需要您提供文件。",
        add: "添加 AI 助理",
        none: "尚无 AI 助理。",
        othersHeading: "同事授权您访问的 AI 助理",
        ownerLine: (owner) => `${owner} 的 AI 助理`,
        allowance: (count, max, name) =>
          max === 1
            ? count === 0
              ? "您可以添加 1 位 AI 助理。"
              : `${name} 是您的 AI 助理。每人可拥有 1 位。`
            : `您已有 ${count} 位 AI 助理，每人最多可有 ${max} 位。`,
        limitReached: (count, max, name) =>
          max === 1
            ? `您已有 AI 助理 ${name}，每人只能有一位。请在 ${name} 的“设置”标签页删除 ${name} 后再添加，或请管理员添加 AI 员工。`
            : `您已有 ${count} 位 AI 助理，已达个人上限。请在某位 AI 助理的“设置”标签页删除后再添加，或请管理员添加 AI 员工。`,
        limitUnfinished: (name) =>
          `${name} 的设置未完成，仍计入您的 AI 助理数量。请先在“未完成”下完成或移除 ${name}。`,
        noneFree: (administrator, provider) =>
          `目前无法添加 AI 助理：此处允许的 AI 助理名额已用完。${administrator ? `请联系 ${provider} 增加名额` : "请联系管理员"}。`,
        noPermission: "您无权添加 AI 助理。请联系管理员。",
        notOffered: "此处不提供 AI 助理。",
        listFailed: "无法加载您的 AI 助理。请检查连接并刷新。",
      },
      employees: {
        title: "AI 员工",
        description:
          "AI 员工属于公司：人员离职后仍可继续工作，有一位监督者，并始终标为 AI。",
        add: "添加 AI 员工",
        none: "尚无 AI 员工。",
        adminsAdd: "由管理员添加 AI 员工。",
        supervisorLine: (supervisor) => `由 ${supervisor} 监督`,
        allowance: (count, max) => `${count} / ${max} 位 AI 员工`,
        limitReached: (max, provider) =>
          `此处允许的 ${max} 位 AI 员工名额已用完。请删除不再需要的 AI 员工，或联系 ${provider} 增加名额。`,
        notOffered: "此处不提供 AI 员工。",
        listFailed: "无法加载 AI 员工。请检查连接并刷新。",
      },
      group: {
        active: "在用",
        paused: "已暂停",
        retired: "已停用",
        unfinished: "未完成",
      },
      unfinished: (name) => `${name} 的设置未完成。`,
      finishSetup: "完成设置",
      remove: "移除",
      unfinishedElsewhere: (name) =>
        `请在添加 ${name} 的浏览器中完成设置，或移除 ${name} 后重新添加。未完成的 AI 助理或 AI 员工会在 24 小时后移除。`,
      unfinishedCompleted: (name) => `${name} 已在此期间完成设置，因此未移除。`,
      access: {
        heading: (name) => `谁可以与 ${name} 一起工作`,
        use: "可对话",
        inspect: "可查看所有工作",
        manage: "可管理",
        useHelp: (name) =>
          `与 ${name} 开始对话并附加文件。打开 ${name} 的文件需要“可查看所有工作”权限。`,
        inspectHelp: (name) =>
          `打开 ${name} 的文件，阅读所有对话、工作标签页和 ${name} 的记忆。`,
        chatOnlyNote: (person, name) =>
          `只有“可对话”权限时，${person} 可与 ${name} 对话并附加文件，但无法打开 ${name} 生成的文件、分配任务或查看他人的对话。`,
        manageHelp: (name) =>
          `更改职责说明、访问权限和文件；暂停、停用或删除 ${name}。`,
        supervisorNote: (name) =>
          `对 ${name} 负责。${name} 的问题会优先发送给此人。`,
        supervisorHint: (name) =>
          `只有能管理 ${name} 的人才能成为监督者。请先授予“可管理”权限。`,
        filesFollow: (name) =>
          `能查看 ${name} 所有工作的人可以打开 ${name} 的文件。还能管理的人可以更改这些文件。`,
        onlyYou: "只有您有访问权限。",
        manageIncludes: "“可管理”包含“可查看所有工作”。",
        manageNeedsInspect: (name) =>
          `要管理 ${name}，您还需要“可查看所有工作”权限。请其他管理者为您授权。`,
        lastManager: (name) =>
          `${name} 至少需要一位管理者。请先授予他人“可管理”权限。`,
        ownManage: "您不能移除自己的“可管理”权限。请其他管理者操作。",
        add: "添加同事",
        searchHint: "输入同事邮箱地址的开头。",
        noResults: "没有匹配的同事。",
        searchHintFull: "输入同事的完整邮箱地址，然后点击“查找”。",
        find: "查找",
        notFoundFull:
          "公司中没有使用该邮箱地址的账户。请检查拼写，或请管理员添加此人。",
        searchFailed: "查找失败。请重试。",
        alreadyHasAccess: (person) => `${person} 已有访问权限。`,
        saved: "已保存。",
        remove: "移除访问权限",
        removeConfirm: (person, name) =>
          `移除 ${person} 对 ${name} 的访问权限？${person} 将无法再看到 ${name} 的对话或文件。`,
      },
      interrupt: {
        title: (name) => `${name} 正在工作`,
        body: (change, name, list) =>
          `${change} 会重启 ${name} 的工作环境，并停止 ${name} 当前的工作：${list}。已保存在 ${name} 文件中的内容会保留。`,
        bodyUnknown: (change, name) =>
          `${change} 会重启 ${name} 的工作环境，并停止 ${name} 当前的工作。已保存在 ${name} 文件中的内容会保留。`,
        grant: "更改访问权限",
        supervise: "更改监督者",
        adopt: "应用更新后的职责说明",
        reply: (person) => `给 ${person} 的回复`,
        work: (objective) => `任务“${objective}”`,
        wait: (name) => `在 ${name} 空闲时应用`,
        now: "停止工作并立即应用",
        pending: (name) =>
          `这项更改会在 ${name} 空闲时应用。请保持此页面打开。`,
      },
      lifecycleAction: {
        suspend: "暂停",
        restore: "恢复",
        archive: "停用",
        delete: "删除",
        adopt: "已应用职责说明",
        supervise: "已更改监督者",
        grant: "已更改访问权限",
      },
      conversations: {
        with: (person) => `与 ${person} 的对话`,
        empty: "尚无对话。",
        untitled: "未命名对话",
        yours: "您的对话",
        running: "工作中",
        ownOnly: "您只能看到自己的对话。能查看所有工作的人可以看到每个对话。",
        loadFailed: "无法加载对话。请刷新重试。",
        noUse: (supervisor) =>
          `您可以阅读对话，但无法开始新对话。请联系 ${supervisor} 获取“可对话”权限。`,
      },
      marker: { ai: "AI", aiAssistant: "AI 助理", aiEmployee: "AI 员工" },
      header: {
        line: (kind, own, owner, supervisor) =>
          kind === "company"
            ? `AI 员工 · 由 ${supervisor} 监督`
            : own
              ? "您的 AI 助理"
              : `${owner} 的 AI 助理 · 由 ${supervisor} 监督`,
        yourAccess: "您的权限：",
      },
      state: {
        working: "正在工作",
        idle: "空闲",
        lastActive: (time) => `上次工作于 ${time}`,
        neverUsed: "尚未使用",
        paused: "已暂停",
        retired: "已停用",
        deleted: "已删除",
        unfinished: "未完成",
        unavailable: "目前无法工作",
        changing: "正在更改",
      },
      notFound: "这位 AI 助理或 AI 员工不可用，或您无权访问。",
      backToList: (kind) =>
        kind === "personal" ? "返回我的助理" : "返回 AI 员工",
      pausedNotice: (name) =>
        `${name} 已暂停。管理者可在 ${name} 的“设置”标签页恢复 ${name}。`,
      retiredNotice: (name) => `${name} 已停用。文件和过往对话仍可阅读。`,
      deletedNotice: (name) => `${name} 已删除。`,
      accessWithdrawn: (name) => `您不再有权访问 ${name}。`,
      tabs: {
        conversations: "对话",
        files: "文件",
        work: "工作",
        memory: "记忆",
        access: "访问权限",
        settings: "设置",
      },
      settings: {
        renamed: "已重命名。",
        assignments: (name) => `${name} 可在工作标签页接受任务`,
        briefNote: (name) =>
          `应用职责说明会关闭 ${name} 的工作环境。下一次对话会启动新环境。未完成的任务需要管理者在工作标签页确认后，${name} 才能继续执行。`,
        briefApplied: "已应用职责说明。",
        useCustomAgent: "改为应用您的某个自定义智能体",
        pauseConfirm: (name) =>
          `这会暂停 ${name} 的工作。对话、文件和记忆会保留。您随时可恢复 ${name}。`,
        runningNow: (name) => `${name} 正在工作。这项工作会停止。`,
        retireConfirm: (name, kind) =>
          kind === "personal"
            ? `这会结束此角色。当前有权限的人仍可阅读过往对话和文件。删除 ${name} 前，${name} 仍计入您的 AI 助理数量。`
            : `这会结束此角色。当前有权限的人仍可阅读过往对话和文件。删除 ${name} 前，${name} 仍计入允许的 AI 员工数量。`,
        deleteConfirm: (name, kind) =>
          kind === "personal"
            ? `这会删除 ${name}、${name} 的记忆及全部文件，无法撤销。空出的位置可立即用于新的 AI 助理。`
            : `这会删除 ${name}、${name} 的记忆及全部文件，无法撤销。空出的位置可立即用于新的 AI 员工。`,
        deleteUnverified: (name, provider) =>
          `${name} 的文件未能通过检查，因此现在无法删除；${provider} 可稍后删除这些文件。${name} 将被删除，不再计入允许的数量。`,
        typeName: (name) => `输入 ${name} 以确认`,
        deleting: (name) => `正在删除 ${name}…`,
        deleted: (name) => `${name} 已删除。`,
        deletedClearing: (name, kind) =>
          kind === "personal"
            ? `${name} 已删除。文件仍在清理；几分钟后即可空出位置用于新的 AI 助理。`
            : `${name} 已删除。文件仍在清理；几分钟后即可空出位置用于新的 AI 员工。`,
        abandonConfirm: (name) => `放弃这项更改？${name} 会保持更改前的状态。`,
        abandonRefused: "暂时无法放弃这项更改。请重试。",
        employeeName: "新 AI 员工的名字",
        employeeAdded: (name) => `已添加 ${name}。`,
        automatic: (name) =>
          `不会按时间表启动 ${name}。只有在有人发送消息或启动任务时，${name} 才会工作。`,
      },
      administratorSupport: "管理员",
      providerFallback: "服务提供方",
      unavailable: {
        serverNotReady:
          "服务器尚未设置好，暂时无法安全运行 AI 助理和 AI 员工。",
        serverCheckFailed:
          "服务器的安全检查失败，AI 助理和 AI 员工暂时无法启动。他们的文件会保留。",
        agentsPaused: (provider) => `${provider} 已暂停 AI 助理和 AI 员工。`,
        serverMoved: (name, provider) =>
          `${name} 暂时无法工作：服务已迁移到新服务器，${provider} 需要确认迁移。${name} 的文件会保留。`,
        filesUnchecked: (name) =>
          `${name} 的文件未通过安全检查，因此无法工作。文件会保留，其他内容不受影响。`,
        agentsWait: "在此之前，AI 助理和 AI 员工无法工作。",
        diskTooSmall: (provider) =>
          `文件暂时不可用：部署的存储小于配置要求。在 ${provider} 修复前，对话可以继续，但无法使用文件。`,
        storageMissing: (provider) =>
          `文件暂时不可用：存储尚未设置。在 ${provider} 修复前，对话可以继续，但无法使用文件。`,
        storageUnreadable: (provider) =>
          `文件暂时不可用：无法读取存储。其中的所有内容都已保留。在 ${provider} 修复前，对话可以继续，但无法使用文件。`,
        serverNotReadyFiles: (provider) =>
          `文件暂时不可用：服务器尚未完成安全保存文件的设置。在 ${provider} 修复前，对话可以继续，但无法使用文件。`,
        serverCheckFailedFiles: (provider) =>
          `文件暂时不可用：服务器安全检查未通过。所有文件都已保留。在 ${provider} 修复前，对话可以继续，但无法使用文件。`,
        next: (provider) => `请联系 ${provider} 修复此问题。`,
        reference: (code) => `支持参考：${code}`,
        askAdmin: "请联系管理员。",
      },
      unconfirmed: "尚未确认更改已保存。请重试；不会重复执行。",
      abandon: "放弃这项更改",
      abandoned: "先前的更改已放弃。未发生任何更改。",

      description: "拥有独立主目录和记忆的持久化个人与公司智能体。",
      legacyUnavailable: "持久化实例需要启用智能体管理并配置合格的存储空间。",
      name: "名字",
      custody: "归属",
      personal: "个人",
      company: "公司",
      supervisor: "监督者",
      definition: "定义",
      start: "新建对话",
      home: "打开文件",
      spaces: "浏览空间和项目",
      manage: "管理实例",
      rename: "重命名",
      suspend: "暂停",
      archive: "停用",
      delete: "删除",
      restore: "恢复",
      adopt: "应用更新后的职责说明",
      supervise: "更改监督者",
      grant: "更新访问权限",
      member: "成员用户 ID",
      use: "使用",
      inspect: "查看",
      manageAccess: "管理",
      grantsNotice:
        "智能体权限与主目录访问权限相互独立。请在空间中编辑资源权限。",
      retention:
        "这些操作停止智能体环境，但保留主目录数据和人员权限。共享项目也会保留。",
      pending: (name) =>
        `这项更改尚未完成：尚未确认 ${name} 的工作环境已关闭。更改完成前，${name} 无法工作。`,
      retry: "重试",
      refresh: "刷新",
      loading: "加载中…",
      none: "暂无实例。",
      more: "加载更多",
      memory: "实例记忆",
      memoryNotice: (name) =>
        `${name} 在不同对话间保留的记忆。能查看所有工作的人都可阅读。不包含任何人的私人记忆。`,
      fact: "新增事实",
      addFact: "添加事实",
      removeFact: "移除事实",
      clearMemory: "清空记忆",
      exportMemory: "导出记忆",
      importMemory: "导入记忆",
      replaceMemory: "替换记忆",
      confirm: "确认操作",
      confirmMemory: (name) => `这将更改 ${name} 的全部记忆，且无法撤销。`,
      companyCopy: "创建公司副本",
      companyCopyNotice:
        "创建独立的公司实例后，请在空间中明确复制所选主目录文件，并导出和导入记忆。凭据不会被复制。",
      status: "状态",
      active: "运行中",
      suspended: "已暂停",
      archived: "已归档",
      deleted: "已移除",
      provisioning: "配置中",
      instructions: "职责说明",
      businessDefinition: "定义名称",
      useOnly: "使用权限允许访问您自己的非受保护对话；实例记忆需要查看权限。",
      operationError: "智能体操作失败。请刷新后再重试。",
      source: "原始定义",
      memoryUnavailable: (name) => `目前无法显示 ${name} 的记忆。`,
    },
    storageSpaces: {
      agentFilesOf: (name) =>
        `这些是 ${name} 的文件。在 ${name} 的“访问权限”标签页更改谁可以使用文件；在 ${name} 的“设置”标签页暂停、停用或删除 ${name}。`,
      agentFilesRecoveryNote: (name) =>
        `备份、恢复或修复文件会关闭 ${name} 的工作环境。进行中的回复或任务会停止；在备份、恢复或修复完成之前，${name} 无法工作。`,
      agentFilesUnknown:
        "这些文件属于一位 AI 助理或 AI 员工。请在该 AI 助理或 AI 员工的“访问权限”标签页更改谁可以使用文件，并在“设置”标签页暂停、停用或删除该 AI 助理或 AI 员工。",
      agentFilesRecoveryUnknown:
        "备份、恢复或修复会关闭该 AI 助理或 AI 员工的工作环境。进行中的回复或任务会停止；文件操作完成前，工作无法继续。",
      membersHint:
        "AI 助理或 AI 员工的文件位于该 AI 助理或 AI 员工的“文件”标签页。",
      createOff: "此处不能创建新的 Space。",
      confirmAction: "确认操作",
      recovery: "备份与恢复",
      recoveryNotice:
        "这些操作会终止已挂载的环境。备份与恢复暂存数据占用此空间的容量。",
      backup: "创建备份",
      archive: "归档空间",
      deleteSpace: "删除空间",
      restore: "恢复文件",
      consistentBackup: "停止写入后捕获的文件",
      attachedEnvironments: "已挂载的环境",
      retireEnvironments: "终止这些环境",
      providerUnavailable: "存储提供者必须重新连接才能终止访问。",
      pendingOperation: "结果未确认的操作",
      acceptCurrentState: "接受当前文件状态",
      restoreNotice: "此操作将以所选备份替换当前文件。当前成员权限保持有效。",
      deleteNotice:
        "此操作删除可见文件并永久关闭空间。保留的备份仍受保留策略管理。",
      acceptStateNotice:
        "上一操作的结果不确定。接受会保持文件原样并清除待恢复状态，不会重放或宣称上一操作完成。请先检查文件与恢复证据。",
      confirmLifecycle: "我已确认此操作的影响",
      recoveryPending: "文件访问已暂停，等待所有者解决待恢复事项。",
      title: "存储空间",
      description: "空间中的文件和文件夹独立于任何对话。",
      unavailable: "存储空间不可用",
      unavailableHint: "此部署尚未启用支持的存储后端。",
      newSpace: "新建空间",
      spaceName: "空间名称",
      custody: "归属",
      personal: "个人",
      company: "公司",
      noSpaces: "没有可用空间",
      openRoot: "根文件夹",
      path: "文件夹路径",
      newFolder: "新建文件夹",
      newFile: "新建文本文件",
      upload: "上传文件",
      loadError: "无法加载此空间。请重新加载以确认当前访问权限。",
      operationError:
        "操作未完成。您的编辑仍保留在此；重试前请检查提示和当前权限。",
      truncated: "已达到文件夹列表上限，可能还有其他条目。",
      readOnly: "只读",
      editorNotice:
        "保存会检查您打开时的版本。如果文件或权限已改变，保存会失败并保留您的草稿。",
      confirmRemove: "移除此条目？",
      removeHint: "文件和空文件夹会被永久移除。非空文件夹必须先清空。",
      newName: "名称或相对路径",
      textContent: "文件内容",
      quota: "空间容量",
      copy: "从其他空间复制",
      copySource: "来源空间",
      sourcePath: "来源文件路径",
      destinationPath: "目标文件路径",
      disclosure: "复制可能让目标空间的成员访问原有的私人数据。",
      disclosureLabel: "我授权向目标空间成员披露此数据",
      limitText:
        "上传和导入支持最大 64 MiB 的文件。文本编辑器支持最大 1 MiB 的 UTF-8 文件。",
      unsupportedEntry: "无法通过浏览器访问此条目。",
      accessLabel: "可用操作",
      utf8Notice: "文本会安全显示；打开活动内容文件时会下载。",
      back: "上级文件夹",
      empty: "此文件夹为空。",
      reload: "重新加载",
      noEditor: "二进制和不支持的文本文件可以下载。",
    },
    extensions: {
      fileDestinationsUnavailable: "无法确认文件保存位置，请刷新后重试。",
      navigation: "扩展",
      pageLoading: "正在加载扩展…",
      pageUnavailable: "扩展页面不可用",
      pageUnavailableHint: "此页面未注册，或插件已停用、未能加载。",
      reload: "重新加载",
      viewFailed: "扩展界面加载失败，请刷新重试。",
      actionFailed: "暂时无法执行扩展操作，请重试。",
    },
    // Locale meta
    locale: {
      localName: "中文",
    },

    // Common
    common: {
      home: "首页",
      settings: "设置",
      delete: "删除",
      edit: "编辑",
      rename: "重命名",
      renameFailed: "重命名会话失败。",
      share: "分享",
      openInNewWindow: "在新窗口打开",
      close: "关闭",
      more: "更多",
      search: "搜索",
      loadMore: "加载更多",
      download: "下载",
      thinking: "思考",
      artifacts: "文件",
      public: "公共",
      custom: "自定义",
      notAvailableInDemoMode: "在演示模式下不可用",
      loading: "加载中...",
      version: "版本",
      lastUpdated: "最后更新",
      code: "代码",
      preview: "预览",
      cancel: "取消",
      save: "保存",
      install: "安装",
      create: "创建",
      import: "导入",
      export: "导出",
      exportAsMarkdown: "导出为 Markdown",
      exportAsJSON: "导出为 JSON",
      exportSuccess: "对话已导出",
      exportFailed: "导出对话失败。",
      regenerate: "重新生成",
      editAndRerun: "编辑并重新运行",
      updateAndRerun: "更新并重新运行",
      editRerunWarning:
        "重新运行只恢复对话状态，不会撤销文件、Memory 或外部操作。",
      branch: "分叉",
      showArtifacts: "查看此对话的文件",
      browser: "浏览器",
      showBrowser: "打开浏览器面板",
    },

    runProgress: {
      residentPreparing: (name) => `正在准备 ${name}…`,
      residentStarting: (name) => `正在启动 ${name} 的工作环境…`,
      residentStartingLong: (name) =>
        `仍在启动 ${name} 的工作环境。一段时间未使用后，启动最多可能需要两分钟。`,
      residentStartFailed: (name, support) =>
        `${name} 的工作环境未能启动。请再次发送消息；如果持续发生，请联系 ${support}。`,
      preparing: "正在准备工作区…",
      workspaceStarting: "正在启动新的工作区…",
      thinking: "思考中…",
    },

    runDuration: {
      reasoning: "思考过程",
      working: "执行中…",
      completedIn: (duration) => `本次任务耗时 ${duration}`,
      description: "任务总耗时，包括模型推理、工具调用和等待时间。",
      lessThanSecond: "不足 1 秒",
      hours: (value) => `${value} 小时`,
      minutes: (value) => `${value} 分`,
      seconds: (value) => `${value} 秒`,
      separator: " ",
    },

    // Home
    home: {
      docs: "文档",
      blog: "博客",
    },

    // Welcome
    welcome: {
      greeting: "你好，欢迎回来！",
      description: `欢迎使用 ${product}。通过内置和自定义的 Skills，\n${product} 可以帮你搜索网络、分析数据，还能为你生成幻灯片、\n图片、视频、播客及网页等，几乎可以做任何事情。`,
      startersLabel: "可以从这些开始",
      startersHint: "选择一个即可填入输入框——在你准备好之前不会发送。",

      createYourOwnSkill: "创建你自己的 Agent SKill",
      createYourOwnSkillDescription: `创建你的 Agent Skill 来释放 ${product} 的潜力。通过自定义技能，${product}\n可以帮你搜索网络、分析数据，还能为你生成幻灯片、\n网页等作品，几乎可以做任何事情。`,
    },

    // Clipboard
    clipboard: {
      copyToClipboard: "复制到剪贴板",
      copiedToClipboard: "已复制到剪贴板",
      failedToCopyToClipboard: "复制到剪贴板失败",
      linkCopied: "链接已复制到剪贴板",
    },

    artifactEditing: {
      unsaved: "未保存",
      saving: "正在保存...",
      saved: "文件已保存",
      exit: "退出编辑",
      discard: "放弃修改",
      discardChanges: "要放弃对此文件的未保存修改吗？",
      conflict: "开始编辑后文件已发生变化。请放弃草稿并重新加载后再保存。",
      conflictShort: "远端已更新",
      runInProgress: "请等待当前 Agent 运行结束后再保存。",
      saveFailed: "保存文件失败",
    },

    artifactPreview: {
      limited: (previewSize, totalSize) =>
        totalSize
          ? `当前显示 ${totalSize} 中的前 ${previewSize}。`
          : `当前显示前 ${previewSize}。`,
      loadFullFile: "加载完整文件",
      loadingFullFile: "正在加载完整文件...",
      previewFailed: "无法预览此文件，但仍可下载原始文件。",
      viewSource: "查看原始文件",
      missingTarget: "该链接没有指明要展示哪个文件。",
    },

    artifactViews: {
      unsupported: "此文件不是受支持的结果视图，仍可访问原始文件。",
      imageUnavailable: "图像不可用或超出显示限制。",
      table: "结果表格",
      exports: "文件",
      exportsHint: "仅保存或共享所选文件，不会自动包含源文件和图像。",
      checkingFiles: "正在检查文件是否可用...",
      filesUncertain: "部分文件暂时无法检查，请重试。",
      noAvailableFiles: "当前没有可用的已展示下载文件。",
      retry: "重新检查",
      collection: (name) => `保存和共享位置：${name}`,
      ignoredCollection: "建议的文件夹不可用，将使用默认位置。",
    },

    artifactArchive: {
      downloadCurrent: (count) => `下载当前版本（${count} 个文件）`,
      currentVersionNotice:
        "文件列表来自此回复；内容为当前版本，可能已发生变化。",
      downloadFailed: "文件压缩包下载失败。",
    },

    // Citations
    citations: {
      sourcesSummary: (count) => `使用了 ${count} 个来源`,
      citeCount: (count) => `${count} 次引用`,
      copyReference: (title) => `复制 ${title} 引用`,
      copiedReference: (title) => `已复制 ${title} 引用`,
    },

    // Workspace Changes
    workspaceChanges: {
      title: "工作区变更",
      editedTitle: (count) => `已编辑 ${count} 个文件`,
      badge: (count, additions, deletions) =>
        `${count} 个文件已更改 +${additions} -${deletions}`,
      viewChanges: "查看更改",
      created: "新增",
      modified: "修改",
      deleted: "删除",
      openFile: "打开文件",
      loading: "正在加载工作区变更...",
      noChanges: "没有记录到工作区变更。",
      diffUnavailable: "无法展示 diff",
      binaryUnavailable: "二进制文件，无法展示 diff。",
      largeUnavailable: "文件过大，已省略 diff。",
      sensitiveUnavailable: "敏感路径，已隐藏内容。",
      truncatedUnavailable: "变更集过大，已省略 diff。",
      symlinkUnavailable: "符号链接变更，无法展示 diff。",
      truncatedSummary: "部分变更已被截断。",
    },

    artifactDelivery: {
      verificationTitle: "无法验证文件交付",
      verificationDescription:
        "输出检查未完成。请先检查文件，再确认此回复所述的任务完成情况。",
      title: (count) => `有 ${count} 个文件没有附在上面这条回复里`,
      description: () =>
        "可在这里打开或下载。上面的回答可能看起来像文件已经附上了。",
      shownOfTotal: (shown, total) => `共 ${total} 个，显示前 ${shown} 个。`,
      receiptToast:
        "文件已生成，但我们无法确认它们已保存到这条对话。请刷新页面确认。",
    },

    // Input Box
    inputBox: {
      agentFile: {
        attach: (name) => `给 ${name} 附加文件`,
        hint: (name) =>
          `附加的文件会保存在 ${name} 的文件中。可以查看 ${name} 所有工作的人都可以打开这些文件。`,
        sendWaits: "请等待文件保存完成。",
        uploading: (percent) => `正在上传… ${percent}%`,
        saved: (name) => `已保存在 ${name} 的文件中`,
        savedAs: (file) => `已保存为 ${file}`,
        refused: (file) => `${file} 未能保存。请重试。`,
        noAccess: (name) => `您已无法向 ${name} 提供文件。`,
        tooLarge: (file, max) => `${file} 超过单个文件的大小上限 ${max}。`,
      },
      placeholder: "今天我能为你做些什么？",
      disclaimer: "内容由AI生成，重要信息请务必核查",
      assistantDisclaimer: "这是 AI，可能会出错。",
      createSkillPrompt:
        "我们一起用 skill-creator 技能来创建一个技能吧。先问问我希望这个技能能做什么。",
      addAttachments: "添加附件",
      inputPolish: "优化输入",
      inputPolishing: "正在优化输入...",
      inputPolishNoChanges: "当前输入已经足够清晰。",
      inputPolishFailed: "优化输入失败。",
      inputPolishUndo: "撤销优化",
      inputPolishCancel: "取消优化",
      voiceInputStartLabel: "语音输入",
      voiceInputStopLabel: "停止语音输入",
      voiceInputStart: `语音输入。${product} 只接收转写文本，音频由浏览器或系统语音服务处理。`,
      voiceInputStop: "停止语音输入",
      voiceInputListening: "正在聆听... 点击停止语音输入。",
      voiceInputUnsupported:
        "当前浏览器不支持语音输入。建议使用 Chrome 或 Edge。",
      voiceInputPermissionDenied: "麦克风权限被拒绝。请允许麦克风访问后重试。",
      voiceInputMicrophoneUnavailable: "未检测到麦克风。请检查设备输入后重试。",
      voiceInputUnsupportedLanguage: "当前浏览器不支持该语言的语音输入。",
      voiceInputNetworkError: "无法连接浏览器语音识别服务。",
      voiceInputNoSpeech: "没有检测到语音，请重试。",
      voiceInputFailed: "语音输入失败，请重试。",
      mode: "模式",
      flashMode: "闪速",
      flashModeDescription: "快速且高效的完成任务，但可能不够精准",
      reasoningMode: "思考",
      reasoningModeDescription: "思考后再行动，在时间与准确性之间取得平衡",
      proMode: "Pro",
      proModeDescription: "思考后再执行，获得更精准的结果，可能需要更多时间",
      ultraMode: "Ultra",
      ultraModeDescription:
        "继承自 Pro 模式，展示可见的任务计划，并可调用子代理分工协作，适合复杂多步骤任务，能力最强",
      reasoningEffort: "推理深度",
      reasoningEffortMinimal: "最低",
      reasoningEffortMinimalDescription: "检索 + 直接输出",
      reasoningEffortLow: "低",
      reasoningEffortLowDescription: "简单逻辑校验 + 浅层推演",
      reasoningEffortMedium: "中",
      reasoningEffortMediumDescription: "多层逻辑分析 + 基础验证",
      reasoningEffortHigh: "高",
      reasoningEffortHighDescription: "全维度逻辑推演 + 多路径验证 + 反推校验",
      modelsLoading: "正在加载模型…",
      modelsUnavailable:
        "没有可用模型。请在账户设置中添加服务商密钥，或联系管理员。",
      modelsLoadFailed: "模型加载失败。请重试，草稿会保留。",
      modelsRetry: "重新加载模型",
      searchModels: "搜索模型...",
      surpriseMe: "小惊喜",
      surpriseMePrompt: "给我一个小惊喜吧",
      followupLoading: "正在生成可能的后续问题...",
      followupConfirmTitle: "发送建议问题？",
      followupConfirmDescription: "当前输入框已有内容，选择发送方式。",
      followupConfirmAppend: "追加并发送",
      followupConfirmReplace: "替换并发送",
      suggestionPlaceholderRequired: "发送前请先填写建议模板中的占位内容。",
      goalCommandDescription: "设置、查看或清除当前目标",
      compactCommandDescription: "压缩早期上下文，保留完整聊天记录",
      goalLabel: "目标",
      goalContinuing: "续跑中 {count}/{max}",
      goalContinuationTooltip:
        "为达成目标已自动续跑 {count}/{max} 次，达上限后自动停止",
      goalSet: "目标已设置。",
      goalCleared: "目标已清除。",
      goalNone: "当前没有目标。",
      goalActive: "当前目标：{goal}",
      goalFailed: "目标命令执行失败。",
      goalTooLong: "目标过长，请控制在 {max} 个字符以内。",
      goalLengthCounter: "目标长度：{length}/{max} 字符",
      compactSuccess:
        "已压缩早期上下文。完整聊天记录仍保留，后续模型将基于摘要和最近消息继续。",
      compactSkipped: "当前上下文还不需要压缩。",
      compactFailed: "上下文压缩失败。",
      suggestions: [
        {
          suggestion: "写作",
          prompt: "撰写一篇关于[主题]的博客文章",
          icon: PenLineIcon,
        },
        {
          suggestion: "研究",
          prompt: "深入浅出的研究一下[主题]，并总结发现。",
          icon: MicroscopeIcon,
        },
        {
          suggestion: "收集",
          prompt: "从[来源]收集数据并创建报告。",
          icon: ShapesIcon,
        },
        {
          suggestion: "学习",
          prompt: "学习关于[主题]并创建教程。",
          icon: GraduationCapIcon,
        },
      ],
      suggestionsCreate: [
        {
          suggestion: "网页",
          prompt: "生成一个关于[主题]的网页",
          icon: CompassIcon,
        },
        {
          suggestion: "图片",
          prompt: "生成一个关于[主题]的图片",
          icon: ImageIcon,
        },
        {
          suggestion: "视频",
          prompt: "生成一个关于[主题]的视频",
          icon: VideoIcon,
        },
        {
          type: "separator",
        },
        {
          suggestion: "技能",
          prompt:
            "我们一起用 skill-creator 技能来创建一个技能吧。先问问我希望这个技能能做什么。",
          icon: SparklesIcon,
        },
      ],
      pleaseWaitStreaming: "请等待当前响应完成。",
    },

    chat: {
      filesFull: (support) =>
        `您的文件存储已满，无法开始新对话。请删除“我的文件”中的文件或旧对话，然后重新开始对话。如果仍然不够，请联系${support}增加存储。`,
      filesPreparing: (support) =>
        `您的文件正在准备中。目前对话可以继续，但无法使用这些文件。如果超过一小时仍未完成，请联系${support}。`,
      filesNoSeat: (administrator, provider) =>
        administrator
          ? `您的账号无法使用文件：这里的账号数超过了席位数。请联系 ${provider} 增加席位。`
          : "您的账号无法使用文件，因此对话可以继续，但无法使用文件。请联系管理员。",
      filesUnavailable:
        "文件暂时不可用。修复前，对话可以继续，但无法使用文件。",
      myFilesUnchecked:
        "您的文件未通过安全检查，因此对话可以继续，但无法使用这些文件。其中的所有内容都已保留。",
    },
    auth: { noSeats: "这里没有空闲席位可供新账号使用。请联系管理员。" },

    // Sidebar
    sidebar: {
      myAssistants: "我的 AI 助理",
      aiEmployees: "AI 员工",
      assistantsOff: "此处不提供 AI 助理。",
      aiEmployeesOff: "此处不提供 AI 员工。",
      agentsUnavailable: "AI 助理和 AI 员工暂时不可用。",
      newChat: "新对话",
      chats: "对话",
      channels: "渠道",
      recentChats: "最近的对话",
      demoChats: "演示对话",
      agents: "智能体",
      scheduledTasks: "定时任务",
      agentsDisabledTooltip: "功能未启用",
      files: "文件",
    },

    files: {
      saveFull: (support) =>
        `您的文件存储已满，无法保存到“我的文件”。请删除“我的文件”中的文件或旧对话，然后重试。如果仍然不够，请联系${support}增加存储。`,
      busy: "您的文件正在更改中。请稍后重试。",
      title: "我的文件",
      filterLabel: "查找已加载的文件",
      filterPlaceholder: "名称或文件夹",
      clearFilter: "清除筛选",
      sortLabel: "文件排序",
      sortName: "名称",
      sortNewest: "最新",
      foundCount: (found, loaded) =>
        `在已加载的 ${loaded.toLocaleString()} 个文件中显示 ${found.toLocaleString()} 个`,
      noMatches: "已加载的文件中没有匹配项",
      noMatchesHint: "尝试其他名称或文件夹，或清除筛选。",
      description: "你在各个对话中保存下来的文件。你可以在任何对话中使用它们。",
      empty: "还没有保存任何文件",
      emptyHint: "在对话中保存一份报告或上传的文件，下次就能在这里找到。",
      loadFailed: "无法加载你的文件",
      retry: "重试",
      name: "名称",
      folder: "文件夹",
      size: "大小",
      modified: "修改时间",
      actions: "操作",
      download: "下载",
      delete: "删除",
      deleteTitle: "删除文件？",
      deleteConfirm: (name) => `删除 ${name}？此操作无法撤销。`,
      deleting: "删除中…",
      deleted: (name) => `已删除 ${name}`,
      deleteFailed: "无法删除该文件，请重试。",
      truncated: (count) =>
        `仅显示前 ${count.toLocaleString()} 个文件。删除一些后可以看到其余的。`,
      saveToMyFiles: "保存到我的文件",
      saving: "保存中…",
      saved: (name) => `已将 ${name} 保存到我的文件`,
      savedToFolder: (name, folder) =>
        `已将 ${name} 保存到我的文件的 ${folder} 文件夹`,
      savedMany: (count) => `已将 ${count} 个文件保存到我的文件`,
      savedManyToFolder: (count, folder) =>
        `已将 ${count} 个文件保存到我的文件的 ${folder} 文件夹`,
      savedSome: (kept, total) =>
        `已保存 ${kept}/${total} 个，其余保存失败，请重试。`,
      saveFailed: "无法保存到我的文件，请重试。",
      openMyFiles: "打开我的文件",
      pageTitle: "文件",
    },

    shared: {
      full: (support) =>
        `公司的共享文件存储已满，无法共享此文件。请移除您共享且不再需要的文件，或联系${support}增加存储。`,
      busy: "共享文件正在更改中。请稍后重试。",
      unchecked: "共享文件未通过安全检查，无法打开。其中的所有内容都已保留。",
      title: "共享",
      description:
        "公司里任何人共享的文件。这里的每个人都能在任何对话中打开它们。",
      empty: "还没有共享任何文件",
      emptyHint:
        "在对话中的报告上，或在你的文件上，使用「共享给所有人」，它就会出现在这里，供公司里的每个人查看。",
      loadFailed: "无法加载共享的文件",
      publishedBy: (who) => `由 ${who} 共享`,
      publishedByNobody: "没有共享者记录",
      publisher: "共享者",
      published: "共享时间",
      remove: "移除",
      removeTitle: "从共享中移除？",
      removeConfirm: (name) =>
        `从共享中移除 ${name}？公司里的人将无法再在这里打开它。你自己的副本（如果有）会保留。`,
      removing: "移除中…",
      removed: (name) => `已从共享中移除 ${name}`,
      removeFailed: "无法移除，请重试。",
      truncated: (count) => `仅显示前 ${count.toLocaleString()} 个文件。`,
      shareWithEveryone: "共享给所有人",
      sharing: "共享中…",
      shared: (name) => `已将 ${name} 共享给公司里的所有人`,
      sharedInFolder: (name, folder) =>
        `已将 ${name} 共享给公司里的所有人，放在 ${folder} 文件夹`,
      sharedMany: (count) => `已将 ${count} 个文件共享给公司里的所有人`,
      sharedManyInFolder: (count, folder) =>
        `已将 ${count} 个文件共享给公司里的所有人，放在 ${folder} 文件夹`,
      sharedSome: (published, total) =>
        `已共享 ${published}/${total} 个，其余共享失败，请重试。`,
      shareFailed: "无法共享，请重试。",
      openShared: "打开共享",
      undo: "撤销",
      undone: (name) => `已将 ${name} 撤出共享`,
      alreadyShared: (name) => `${name} 已共享给所有人`,
      alreadySharedAs: (name, sharedAs) =>
        `${name} 已以 ${sharedAs} 的名称共享给所有人`,
      alreadySharedMany: (count) => `这 ${count} 个文件均已共享给所有人`,
    },

    backgroundTasks: {
      label: "后台任务",
      title: "后台任务",
      description: "当前对话中的 MCP 长程任务。",
      active: "进行中",
      recent: "最近任务",
      empty: "暂无后台任务",
      emptyHint: "在当前对话中启动的 MCP 长程任务会显示在这里。",
      loadFailed: "无法加载后台任务",
      retry: "重试",
      cancel: "取消任务",
      cancelling: "正在取消…",
      cancelFailed: "取消任务失败",
      cancellationRetrying: (attempt) =>
        `第 ${attempt} 次取消失败；${product} 将继续重试。`,
      notificationRetrying: (attempt) =>
        `第 ${attempt} 次聊天通知失败；${product} 将退避后重试。`,
      notificationStopped: "聊天通知因反复失败或永久拒绝，已停止重试。",
      trackingDegraded: `状态检查有所延迟，${product} 仍在重试。`,
      viewDetails: "查看详情",
      hideDetails: "收起详情",
      detailsFailed: "无法加载任务详情",
      result: "结果",
      resultArtifact: "结果产物",
      inputRequired: "需要输入",
      inputUnavailable: "当前集成暂时无法将你的回复发回远端任务。",
      lastPollError: "最近一次状态错误",
      created: (time) => `开始于${time}`,
      updated: (time) => `更新于${time}`,
      status: {
        submitted: "已提交",
        working: "进行中",
        inputRequired: "需要输入",
        completed: "已完成",
        failed: "已失败",
        cancelled: "已取消",
      },
    },

    subagentBatches: {
      label: "批处理",
      title: "子智能体批处理",
      description: "面向大量独立条目的持久化、可恢复执行。",
      workerUnavailable:
        "批处理 worker 未运行。历史批次仍可查看和导出，当前为只读模式。",
      empty: "暂无子智能体批处理",
      emptyHint: "当前对话通过 batch_task 提交的批处理会显示在这里。",
      loadFailed: "无法加载子智能体批处理",
      active: "进行中",
      recent: "最近任务",
      pause: "暂停",
      resume: "继续",
      cancel: "取消",
      retryItem: "重试",
      exportResults: "导出 JSONL",
      viewItems: "查看条目",
      hideItems: "收起条目",
      itemsFailed: "无法加载批处理条目",
      progress: (completed, total) => `${completed}/${total} 已结束`,
      limits: (live, running) => `存活 ${live} · 运行 ${running}`,
      status: {
        queued: "排队中",
        running: "运行中",
        paused: "已暂停",
        completed: "已完成",
        failed: "已失败",
        cancelled: "已取消",
      },
    },

    // 定时任务
    scheduledTasks: {
      scheduleType: {
        cron: "重复",
        once: "单次",
      },
      preset: {
        label: "重复方式",
        hourly: "每小时",
        daily: "每天",
        weekly: "每周",
        monthly: "每月",
        custom: "自定义 cron",
      },
      fields: {
        minute: "分钟",
        time: "时间",
        weekday: "在",
        dayOfMonth: "几号",
        cron: "cron 表达式",
        cronPlaceholder: "0 9 * * *",
        runAt: "运行时间",
        timezone: "时区",
      },
      weekdays: {
        mon: "周一",
        tue: "周二",
        wed: "周三",
        thu: "周四",
        fri: "周五",
        sat: "周六",
        sun: "周日",
      },
      preview: "预览",
      cronHelp: "打开 crontab.guru",
      create: {
        title: "创建定时任务",
        taskTitle: "任务标题",
        prompt: "提示词",
        submit: "创建",
        fillRequired: "请填写所有必填项",
      },
      context: {
        fresh: "新线程",
        reuse: "复用线程",
        threadIdPlaceholder: "线程 ID",
        reuseNoticeTitle: "使用该线程的历史对话",
        reuseNoticeDescription: `如果触发时该线程正在运行，${product} 会将本次执行排队，并在线程空闲后启动；超过配置的最长等待时间后会标记为失败。`,
      },
      scheduler: {
        offTitle: "定时执行已关闭",
        offDescription:
          "你的定时任务仍然保存着，但没有任何程序在执行它们，所显示的时间不会自动触发。你仍可以用“立即触发”手动运行一次。需要管理员重新开启定时执行。",
        unavailableTitle: "没有任何程序在执行这些定时任务",
        unavailableDescription:
          "你的定时任务仍然保存着，但此工作区没有调度程序，所显示的时间不会触发。需要管理员添加调度程序。",
        stoppedTitle: "调度程序已停止",
        stoppedDescription:
          "定时执行已开启，但此处没有调度程序在运行，所显示的时间不会自动触发。你仍可以用“立即触发”手动运行一次。需要管理员将其重新启动。",
        overdue: "该时间已过去",
      },
      filters: {
        allStatuses: "全部状态",
        enabled: "已启用",
        paused: "已暂停",
        completed: "已完成",
        failed: "已失败",
        allTypes: "全部类型",
        cron: "定时",
        once: "单次",
      },
      detail: {
        contextMode: "上下文模式",
        thread: "线程",
        lastThread: "上个线程",
        schedule: "调度",
        nextRun: "下次运行",
        lastRun: "上次运行",
        lastRunId: "上次运行 ID",
        lastError: "上次错误",
        runsCount: "{count} 次运行",
        runsCountOne: "{count} 次运行",
        noRuns: "暂无运行",
        noSelection: "未选择定时任务",
        filteredByThread: "按线程筛选：{id}",
        loadFailed: "加载定时任务失败",
      },
      actions: {
        edit: "编辑",
        cancelEdit: "取消编辑",
        pause: "暂停",
        resume: "恢复",
        trigger: "立即触发",
        duplicate: "复制",
        duplicateTitleSuffix: "（副本）",
        delete: "删除",
      },
      deleteConfirm: "确定要删除该定时任务吗？此操作不可撤销。",
      errors: {
        create: "创建定时任务失败",
        update: "更新定时任务失败",
        pause: "暂停定时任务失败",
        resume: "恢复定时任务失败",
        trigger: "触发定时任务失败",
        delete: "删除定时任务失败",
      },
      edit: {
        titlePlaceholder: "编辑标题",
        promptPlaceholder: "编辑提示词",
        submit: "保存编辑",
      },
      status: {
        enabled: "已启用",
        paused: "已暂停",
        running: "运行中",
        completed: "已完成",
        failed: "已失败",
        cancelled: "已取消",
      },
      runTrigger: { scheduled: "定时", manual: "手动" },
      runStatus: {
        queued: "排队中",
        launching: "启动中",
        running: "运行中",
        success: "成功",
        failed: "失败",
        skipped: "跳过",
        interrupted: "已中断",
      },
      recipes: {
        label: "快速创建",
        trending: {
          title: "GitHub Trending 日榜",
          desc: "总结今日 Trending 前十仓库",
        },
        news: {
          title: "每日科技新闻摘要",
          desc: "收集并总结当日科技要闻",
        },
        issues: {
          title: "GitHub Issue 分诊",
          desc: "分诊某仓库的 open issues（填入 {{repo}}）",
        },
        weekly: {
          title: "每周周报",
          desc: "每周一汇总一周工作",
        },
      },
    },

    // Agents
    agents: {
      title: "智能体",
      description: "创建和管理具有专属 Prompt 与能力的自定义智能体。",
      newAgent: "新建智能体",
      emptyTitle: "还没有自定义智能体",
      emptyDescription: "创建你的第一个自定义智能体，设置专属系统提示词。",
      featureDisabledTitle: "智能体功能未启用",
      featureDisabledDescription: "该功能未在此服务器上启用，请联系管理员。",
      chat: "对话",
      delete: "删除",
      deleteConfirm: "确定要删除该智能体吗？此操作不可撤销。",
      deleteSuccess: "智能体已删除",
      newChat: "新对话",
      createPageTitle: "设计你的智能体",
      createPageSubtitle: "描述你想要的智能体，我来帮你通过对话创建。",
      nameStepTitle: "给新智能体起个名字",
      nameStepHint:
        "只允许字母、数字和连字符，存储时自动转为小写（例如 code-reviewer）",
      nameStepPlaceholder: "例如 code-reviewer",
      nameStepContinue: "继续",
      nameStepInvalidError: "名称无效，只允许字母、数字和连字符",
      nameStepAlreadyExistsError: "已存在同名智能体",
      nameStepNetworkError: "网络请求失败，请检查网络或后端连接",
      nameStepCheckError: "无法验证名称可用性，请稍后重试",
      nameStepCheckErrorWithDetail: "名称校验失败：{detail}",
      nameStepApiDisabledError:
        "服务器未开启自定义智能体管理功能，请联系管理员。",
      nameStepBootstrapMessage:
        "新智能体的名称是 {name}。请先帮我设计它的用途、行为方式和 SOUL.md，再保存它。",
      save: "保存智能体",
      saving: "正在保存智能体...",
      saveRequested: `已提交保存请求，${product} 正在根据当前对话生成并保存初版智能体。`,
      saveHint:
        "你可以在右上角的菜单里随时保存这个智能体，就算目前还只是初稿也可以。",
      saveCommandMessage:
        "请现在根据我们目前已经讨论的全部内容保存这个自定义智能体。这就是我明确的保存确认。如果仍有少量细节缺失，请根据上下文做出合理假设，生成一份简洁的英文初始 SOUL.md，并直接调用 setup_agent，不要再向我索要额外确认。",
      agentCreatedPendingRefresh: `智能体已创建，但 ${product} 暂时还无法读取到它。请稍后刷新当前页面。`,
      more: "更多操作",
      agentCreated: "智能体已创建！",
      startChatting: "开始对话",
      backToGallery: "返回 Gallery",
      settings: "模型设置",
      settingsTitle: "模型设置",
      settingsDescription:
        "为该智能体选择默认模型和生成参数，修改在下一条消息生效。",
      settingsModel: "默认模型",
      settingsModelDefault: "使用全局默认",
      settingsTemperature: "温度",
      settingsTemperatureHint: "0 = 确定性输出，越高越发散（0–2）。",
      settingsMaxTokens: "最大输出 token",
      settingsMaxTokensPlaceholder: "继承模型配置",
      settingsThinking: "思考模式",
      settingsThinkingOn: "开启",
      settingsThinkingOff: "关闭",
      settingsReasoningEffort: "推理强度",
      settingsInherit: "继承",
      settingsSaved: "模型设置已保存",
      settingsInvalidTemperature: "温度必须在 0 到 2 之间",
      settingsInvalidMaxTokens: "最大输出 token 必须为不超过 200,000 的正整数",
    },

    // Breadcrumb
    breadcrumb: {
      workspace: "工作区",
      chats: "对话",
    },

    // Workspace
    workspace: {
      contactSupport: "联系支持",
      providerSupport: (providerName) => `联系 ${providerName} 支持`,
      settingsAndMore: "设置和更多",
      about: `关于 ${product}`,
      aboutCompany: (companyName: string) => `关于 ${companyName}`,
      logout: "退出登录",
      gatewayUnavailable: "网关暂时不可用。",
      gatewayUnavailableRetrying: "正在后台重试…",
      modelLoadFailed:
        "模型列表加载失败，模型选择和 Token 用量信息可能暂时不可用。",
      modelLoadRetry: "重试",
      modelLoadRetrying: "正在重试…",
    },

    // Download all my data
    accountExport: {
      menuItem: "下载我的全部数据",
      title: "下载我的全部数据",
      description:
        "包含你的对话、上传或助手生成的文件、“我的文件”、技能、记忆、定时任务、自定义智能体、使用中及已归档项目的设置、现有项目文档和已保存的偏好设置。不包含已删除的项目和回收站中的项目文档。",
      onlyYours:
        "仅包含你自己的内容：不含他人的任何数据，也不含已保存的密码、密钥或连接设置。",
      closeAnytime:
        "账户数据较多时，准备可能需要一段时间。期间你可以关闭此窗口，稍后回到这里下载。",
      start: "准备下载",
      preparing: "正在准备下载…",
      conversationsProgress: (done: number, total: number) =>
        `对话 ${done} / ${total}`,
      filesProgress: (done: number, total: number) => `文件 ${done} / ${total}`,
      stop: "停止准备",
      ready: "下载内容已准备好。",
      includesUpTo: (time: string) => `包含截至 ${time} 的全部内容。`,
      download: "下载",
      downloadPart: (number: number, count: number) =>
        `下载第 ${number} 部分（共 ${count} 部分）`,
      downloaded: "已下载",
      multiPart:
        "下载内容分为多个部分。请逐一下载，然后全部解压到同一个文件夹。",
      availableUntil: (time: string) =>
        `可下载至 ${time}。开始下载会延长保留时间。`,
      keptWhileDownloading: "正在下载，下载期间会一直保留。",
      allDownloaded:
        "全部内容已下载。此下载副本将在几分钟后删除，在此之前可以重新下载任一部分。你账户中的数据不会被删除。",
      skipped: (count: number) =>
        `有 ${count} 项未能包含在内。下载中的 README.md 列出了具体项目和原因。`,
      deleteNow: "删除此下载",
      deleteFailed: "无法删除此下载，请重试。",
      gone: "之前的下载已被删除。你可以重新准备一个。",
      noSpace: "可用空间不足，无法准备下载。请联系管理员腾出空间。",
      failed: "下载未能准备完成，请重试。",
      tryAgain: "重试",
      busy: "其他人的下载正在准备中，请几分钟后再试。",
      unavailable: "此工作区不支持下载全部数据。",
      loadFailed: "无法获取下载状态。",
    },

    // Conversation
    conversation: {
      noMessages: "还没有消息",
      startConversation: "开始新的对话以查看消息",
      branchCreated: "已创建分叉对话",
      branchFailed: "创建分叉对话失败。",
      streamReplayGap: "部分实时更新已过期，已从持久化状态恢复对话。",
      outlineLabel: "对话章节",
      outlineAttachmentFallback: "图片或文件消息",
    },

    // Chats
    chats: {
      deleteChat: "删除对话？",
      deleteConfirm: (title) => `删除“${title}”及其侧边对话？此操作无法撤销。`,
      deleteFailed: "无法删除对话，请重试。",
      deleting: "正在删除…",
      searchChats: "搜索对话",
      branchLabel: (title, parentTitle) => `${title}，分叉自 ${parentTitle}`,
      loadMoreToSearch: "加载更多以搜索更早的对话",
      loadingMore: "正在加载...",
      loadOlderChats: "加载更早的对话",
      pinChat: "置顶对话",
      unpinChat: "取消置顶",
      pinChatFailed: "更新对话置顶状态失败",
    },

    // Sidecar
    sidecar: {
      title: "侧边对话",
      open: "打开侧边对话",
      close: "关闭侧边对话",
      delete: "删除侧边对话",
      deleteConfirm:
        "确定要删除该侧边对话吗？此操作不可撤销。如果只是想隐藏，请使用顶部的侧边对话开关。",
      deleteSuccess: "侧边对话已删除",
      deleteFailed: "删除侧边对话失败。",
      addToConversation: "添加到对话",
      askInSideChat: "在侧边聊天中提问",
      reference: "引用",
      selectedTextFragment: "{count} 个已选文本片段",
      selectedTextFragments: "{count} 个已选文本片段",
      clearReferences: "清除已选引用",
      emptyTitle: "继续深入追问",
      emptyDescription: "基于引用内容单独追问。",
      placeholder: "继续深入追问...",
      send: "发送",
      sendFailed: "侧边对话发送失败。",
      noContext: "未选择上下文",
      continuing: "继续当前侧边对话",
      selectionCrossesMessages:
        "选区跨越了多条消息，请在同一条回复内选择要引用的文本。",
    },

    // Channels
    channels: {
      title: "渠道",
      connect: "连接",
      modify: "修改",
      reconnect: "重新连接",
      disconnect: "断开连接",
      connected: "已连接",
      notConnected: "未连接",
      pending: "待完成",
      revoked: "已断开",
      disabled: "已停用",
      unconfigured: "未配置",
      unavailable: "当前无法使用渠道连接。",
      unavailableShort: "不可用",
      setupTitle: (name: string) => `连接 ${name}`,
      setupEditTitle: (name: string) => `修改 ${name}`,
      setupDescription:
        "填写当前服务进程需要的配置值。这些内容不会写入 config.yaml。",
      saveAndConnect: "保存并连接",
      saveChanges: "保存修改",
      descriptions: {
        buzz: `通过 ${product} 智能体接收 Buzz 频道消息和私聊。`,
        telegram: `通过 ${product} Bot 接收 Telegram 私聊消息。`,
        slack: "接收 Slack 工作区消息和提及。",
        discord: `通过 ${product} Bot 接收 Discord 服务器消息。`,
        feishu: `通过 ${product} 应用接收飞书和 Lark 消息。`,
        dingtalk: `通过 ${product} Bot 接收钉钉 Stream Push 消息。`,
        wechat: `通过 ${product} Bot 接收微信 iLink 消息。`,
        wecom: `通过 ${product} AI Bot 接收企业微信消息。`,
      },
      connectedAs: (name: string) => `已连接为 ${name}。`,
    },

    // Page titles (document title)
    pages: {
      appName: product,
      chats: "对话",
      newChat: "新对话",
      untitled: "未命名",
    },

    // Tool calls
    toolCalls: {
      moreSteps: (count: number) => `查看其他 ${count} 个步骤`,
      lessSteps: "隐藏步骤",
      executeCommand: "执行命令",
      presentFiles: "展示文件",
      showWebPage: "提供页面链接",
      needYourHelp: "需要你的协助",
      useTool: (toolName: string) => `使用 “${toolName}” 工具`,
      searchFor: (query: string) => `搜索 “${query}”`,
      searchForRelatedInfo: "搜索相关信息",
      searchForRelatedImages: "搜索相关图片",
      searchForRelatedImagesFor: (query: string) => `搜索相关图片 “${query}”`,
      searchOnWebFor: (query: string) => `在网络上搜索 “${query}”`,
      viewWebPage: "查看网页",
      listFolder: "列出文件夹",
      readFile: "读取文件",
      writeFile: "写入文件",
      clickToViewContent: "点击查看文件内容",
      writeTodos: "更新 To-do 列表",
      skillInstallTooltip: `安装技能并使其可在 ${product} 中使用`,
      browserNavigate: (url: string) => `在浏览器中打开 ${url}`,
      browserNavigateGeneric: "在浏览器中打开页面",
      browserClick: "在浏览器中点击元素",
      browserType: "在浏览器中输入内容",
      browserSnapshot: "在浏览器中读取页面",
      browserGetText: "在浏览器中读取页面文本",
      browserBack: "在浏览器中返回上一页",
      browserScreenshot: "浏览器截图",
      browserClose: "关闭浏览器",
    },

    humanInput: {
      answered: "已回答",
      pending: "发送中...",
      readOnly: "只读",
      otherLabel: "其他回答",
      otherPlaceholder: "输入其他回答...",
      submit: "提交",
      emptyError: "请输入回答后再提交。",
      requiredError: "请填写所有必填字段后再提交。",
      requiredA11yLabel: "必填",
      selectPlaceholder: "请选择...",
      answeredValue: (value: string) => `已回答：${value}`,
    },

    uploads: {
      filesFull: (file, support) =>
        `您的文件存储已满，未添加 ${file}。请删除“我的文件”中的文件或旧对话，然后重试。如果仍然不够，请联系${support}增加存储。`,
      uploading: "上传中...",
      uploadingFiles: "文件上传中，请稍候...",
      limitsHint: (
        maxFiles: number,
        maxFileSize: string,
        maxTotalSize: string,
      ) =>
        `添加附件（最多 ${maxFiles} 个，单文件不超过 ${maxFileSize}，总计不超过 ${maxTotalSize}）。支持常规文件类型；macOS .app 应先压缩。`,
      filesTooLarge: (files: string, maxFileSize: string) =>
        `${files} 超过单文件 ${maxFileSize} 的限制，未被添加。`,
      tooManyFiles: (count: number, maxFiles: number) =>
        `有 ${count} 个文件未被添加；一次最多添加 ${maxFiles} 个文件。`,
      totalSizeTooLarge: (count: number, maxTotalSize: string) =>
        `有 ${count} 个文件未被添加；附件总大小不能超过 ${maxTotalSize}。`,
    },

    subtasks: {
      subtask: "子任务",
      executing: (count: number) =>
        `${count > 1 ? "并行" : ""}执行 ${count} 个子任务`,
      in_progress: "子任务运行中",
      completed: "子任务已完成",
      failed: "子任务失败",
    },

    // Token Usage
    tokenUsage: {
      title: "Token 用量",
      label: "Tokens",
      input: "输入",
      output: "输出",
      total: "总计",
      view: "显示方式",
      unavailable:
        "暂无 Token 用量。只有模型成功返回且供应商提供 usage_metadata 时才会显示。",
      unavailableShort: "未返回用量",
      collecting: "统计中",
      note: "顶部总量优先使用后端持久化的线程用量；当当前回复仍在流式返回时，还会叠加可见的进行中用量。每轮和调试用量只来自当前可见消息，可能与平台账单页不完全一致。",
      presets: {
        off: "关闭",
        summary: "总览",
        perTurn: "每轮",
        debug: "调试",
      },
      presetDescriptions: {
        off: "隐藏顶部和会话内的 token 展示。",
        summary: "只在顶部显示当前对话累计 token。",
        perTurn: "显示顶部累计，并为每轮 assistant 回复显示一条汇总 token。",
        debug: "显示顶部累计，并展示按步骤归类的 token 调试信息。",
      },
      finalAnswer: "最终回复",
      stepTotal: "步骤总计",
      sharedAttribution: "该 token 由此步骤中的多个动作共同消耗",
      subagent: (description: string) => `子任务：${description}`,
      startTodo: (content: string) => `开始 To-do：${content}`,
      completeTodo: (content: string) => `完成 To-do：${content}`,
      updateTodo: (content: string) => `更新 To-do：${content}`,
      removeTodo: (content: string) => `移除 To-do：${content}`,
    },

    contextUsage: {
      label: "上下文",
      title: "上下文窗口",
      badgeAriaLabel: (percentage: string) => `上下文窗口已使用 ${percentage}%`,
    },

    // Shortcuts
    shortcuts: {
      searchActions: "搜索操作...",
      noResults: "未找到结果。",
      actions: "操作",
      keyboardShortcuts: "键盘快捷键",
      keyboardShortcutsDescription: `使用键盘快捷键更快地操作 ${product}。`,
      openCommandPalette: "打开命令面板",
      toggleSidebar: "切换侧边栏",
    },

    // Settings
    settings: {
      providerEnablement: "自定义功能需要服务提供方启用。",
      title: "设置",
      description: `根据你的偏好调整 ${product} 的界面和行为。`,
      descriptionForCompany: "根据你的偏好调整工作区的界面和行为。",
      sections: {
        account: "账号",
        appearance: "外观",
        channels: "渠道",
        integrations: "集成",
        memory: "记忆",
        tools: "工具",
        subagents: "子智能体",
        skills: "技能",
        notification: "通知",
        about: "关于",
      },
      memory: {
        title: "记忆",
        description: `${product} 会在后台不断从你的对话中自动学习。这些记忆能帮助 ${product} 更好地理解你，并提供更个性化的体验。`,
        empty: "暂无可展示的记忆数据。",
        rawJson: "原始 JSON",
        exportButton: "导出记忆",
        exportSuccess: "记忆已导出",
        importButton: "导入记忆",
        importConfirmTitle: "导入记忆？",
        importConfirmDescription: "这会用选中的 JSON 备份覆盖当前记忆。",
        importFileLabel: "已选择文件",
        importInvalidFile: "读取记忆文件失败，请选择有效的 JSON 导出文件。",
        importSuccess: "记忆已导入",
        manualFactSource: "手动添加",
        addFact: "添加事实",
        addFactTitle: "添加记忆事实",
        editFactTitle: "编辑记忆事实",
        addFactSuccess: "事实已创建",
        editFactSuccess: "事实已更新",
        clearAll: "清空全部记忆",
        clearAllConfirmTitle: "要清空全部记忆吗？",
        clearAllConfirmDescription:
          "这会删除所有已保存的摘要和事实。此操作无法撤销。",
        clearAllSuccess: "已清空全部记忆",
        factDeleteConfirmTitle: "要删除这条事实吗？",
        factDeleteConfirmDescription:
          "这条事实会立即从记忆中删除。此操作无法撤销。",
        factDeleteSuccess: "事实已删除",
        factContentLabel: "内容",
        factCategoryLabel: "类别",
        factConfidenceLabel: "置信度",
        factContentPlaceholder: "描述你想保存的记忆事实",
        factCategoryPlaceholder: "context",
        factConfidenceHint: "请输入 0 到 1 之间的数字。",
        factSave: "保存事实",
        factValidationContent: "事实内容不能为空。",
        factValidationConfidence: "置信度必须是 0 到 1 之间的数字。",
        noFacts: "还没有保存的事实。",
        summaryReadOnly:
          "摘要分区当前仍为只读。现在你可以清空全部记忆或删除单条事实。",
        memoryFullyEmpty: "还没有保存任何记忆。",
        factPreviewLabel: "即将删除的事实",
        searchPlaceholder: "搜索记忆",
        filterAll: "全部",
        filterFacts: "事实",
        filterSummaries: "摘要",
        noMatches: "没有找到匹配的记忆。",
        markdown: {
          overview: "概览",
          userContext: "用户上下文",
          work: "工作",
          personal: "个人",
          topOfMind: "近期关注（Top of mind）",
          historyBackground: "历史背景",
          recentMonths: "近几个月",
          earlierContext: "更早上下文",
          longTermBackground: "长期背景",
          updatedAt: "更新于",
          facts: "事实",
          empty: "（空）",
          table: {
            category: "类别",
            confidence: "置信度",
            confidenceLevel: {
              veryHigh: "极高",
              high: "较高",
              normal: "一般",
              unknown: "未知",
            },
            content: "内容",
            source: "来源",
            createdAt: "创建时间",
            view: "查看",
          },
        },
      },
      appearance: {
        themeTitle: "主题",
        themeDescription: "跟随系统或选择固定的界面模式。",
        system: "系统",
        light: "浅色",
        dark: "深色",
        systemDescription: "自动跟随系统主题。",
        lightDescription: "更明亮的配色，适合日间使用。",
        darkDescription: "更暗的配色，减少眩光方便专注。",
        languageTitle: "语言",
        languageDescription: "在不同语言之间切换。",
      },
      tools: {
        title: "工具",
        description: "管理 MCP 工具的配置和启用状态。",
        adminRequired: "需要管理员权限才能管理 MCP 工具。",
        empty: "暂无 MCP 工具。",
        addServer: "添加服务器",
        addServerDescription:
          "粘贴 MCP 服务器提供的 JSON 定义。直接的服务器映射和带 `mcpServers` 外层的写法都可以。已有名称请通过“编辑”修改。",
        addServerPlaceholder: `{
  "mcpServers": {
    "my-server": {
      "command": "npx",
      "args": ["-y", "@my-org/my-mcp-server"]
    }
  }
}`,
        serverDefinitionLabel: "MCP 服务器 JSON 定义",
        definitionEmpty: "请粘贴 MCP 服务器定义。",
        definitionInvalidJson: "请输入有效的 JSON。",
        definitionRootNotObject:
          "请输入描述一个或多个 MCP 服务器的 JSON 对象。",
        definitionNoServers: "定义中未找到 MCP 服务器。",
        definitionServerNotObject: "服务器“{name}”的配置必须是 JSON 对象。",
        editServer: "编辑 MCP 服务器",
        editServerDescription:
          "编辑“{name}”的完整 JSON 定义。服务器名称不可修改；如需重命名，请添加新服务器后移除当前服务器。",
        editSingleServer: "每次只能编辑一个 MCP 服务器。",
        editServerNameMismatch: "编辑时请保留现有服务器名称“{name}”。",
        serverAlreadyExists: "MCP 服务器“{name}”已存在，请使用“编辑”。",
        removeServer: "移除 MCP 服务器",
        removeServerDescription:
          "确定从 MCP 配置中移除“{name}”吗？它的工具将不再提供给智能体。",
        unnamedServer: "（空名称）",
      },
      subagents: {
        title: "子智能体",
        description:
          "可由 Lead Agent 和已授权 Custom Agent 派遣、执行边界明确任务的复用型工作智能体。",
        executionNote:
          "每次调用都使用全新的临时上下文，没有独立持久会话或记忆，也不能向用户追问。系统提示词只改变工作方式；工具和技能才提供实际能力。",
        adminNote:
          "你可以查看目录；只有管理员可以添加、编辑、启停或删除子智能体。",
        create: "添加子智能体",
        empty: "暂无可用子智能体。",
        sourceBuiltin: "内置",
        sourceConfig: "config.yaml",
        sourceManaged: "设置页管理",
        conflict: "名称冲突，已从运行时排除",
        overridden: "部分运行参数已被 config.yaml 覆盖",
        createTitle: "添加子智能体",
        editTitle: "编辑子智能体",
        name: "名称",
        nameHint: "仅可使用字母、数字和连字符。",
        displayName: "显示名称",
        descriptionLabel: "派遣说明",
        systemPrompt: "系统提示词",
        model: "模型",
        inheritModel: "继承调用者模型",
        tools: "允许的工具（逗号分隔）",
        skills: "技能（逗号分隔）",
        listModeAll: "继承全部可用项",
        listModeNone: "全部禁用",
        listModeSelected: "仅允许指定名称",
        listNamesPlaceholder: "多个名称用逗号分隔",
        maxTurns: "最大轮次",
        timeout: "超时时间（秒）",
        created: "子智能体已创建",
        saved: "子智能体已保存",
        deleted: "子智能体已删除",
        deleteConfirm:
          "确定删除这个子智能体吗？Custom Agent 可能仍保留对该名称的引用；以同名重建后，这些绑定会重新生效。此操作无法撤销。",
        bindingTitle: "子智能体权限",
        bindingDescription:
          "选择这个 Custom Agent 可以派遣哪些子智能体；服务端会强制执行该范围。",
        allAllowed: "全部已启用子智能体",
        noneAllowed: "不允许使用子智能体",
        selectedAllowed: "仅允许选中的子智能体",
        missing: "已缺失或不可用；取消勾选后移除",
      },
      channels: {
        title: "渠道",
        description: `连接可在浏览器外向 ${product} 发送消息的即时通讯账号。`,
        disabled:
          "当前服务器未启用渠道连接。请联系管理员开启 channel_connections。",
      },
      integrations: {
        title: "集成",
        description: "连接第三方工具和办公生态，让 Agent 能直接使用对应能力。",
        refresh: "刷新",
        install: "安装",
        reinstall: "重新安装",
        installing: "安装中...",
        ready: "就绪",
        pending: "待处理",
        available: "可用",
        unavailable: "不可用",
        connected: "已连接",
        loadFailed: "加载集成状态失败",
        adminRequired: "需要管理员权限才能安装集成。",
        lark: {
          title: "Lark / 飞书 CLI",
          description:
            "安装官方 Lark/Feishu Agent Skills，并在授权后让 Agent 直接使用飞书能力。",
          skillPack: "技能包",
          gatewayCli: "Gateway CLI",
          auth: "授权",
          sandboxRuntime: "沙箱运行时",
          sandboxRuntimeInitContainer: "由 init container 提供",
          sandboxRuntimeBroker: "由 broker sidecar 提供",
          sandboxRuntimeGatewayDownload: "由 Gateway 提供",
          sandboxRuntimeNotReady: "未就绪 —— 对话时 lark-cli 可能不可用",
          notInstalled: "尚未安装",
          skillsInstalled: (installed, expected) =>
            `已安装 ${installed}/${expected} 个技能`,
          installedVersion: (version) => `已安装版本：${version}`,
          updateAvailable: (version) =>
            `有新版本：${version} —— 管理员重新安装会更新 managed Gateway CLI 和技能包`,
          runtimeVersionMismatch:
            "技能包版本与 Gateway 运行时 lark-cli 不一致；管理员重新安装会尝试更新 managed Gateway CLI 并重新对齐技能包",
          authNotConfigured: "尚未连接",
          authConfigured: "凭证已配置（未实时验证）",
          authConfiguredFor: (user) => `${user} · 凭证已配置（未实时验证）`,
          connect: "连接飞书",
          authStarting: "正在打开连接链接...",
          checkingConnection: "正在检查连接状态...",
          connectedAction: "重新连接飞书",
          requestPermissions: "申请新权限",
          alreadyConnected:
            "飞书已连接，无需重复授权。如果授权已过期，刷新状态后可重新连接。",
          changeAppButton: "切换飞书 Bot",
          changeAppTitle: "切换到其他飞书 App",
          changeAppDescription: `把你的 ${product} 账号指向另一个 Lark/飞书 App。只影响你自己的账号，不影响其他用户。`,
          changeAppIdLabel: "App ID",
          changeAppSecretLabel: "App Secret",
          changeAppAuthResetNote:
            "切换时会撤销旧 App 的授权，随后需要授权新 App。",
          changeAppSubmit: "切换 App",
          changeAppReRegister: "在浏览器重新注册",
          changeAppSwitched: "已切换飞书 App。请重新连接以授权新 App。",
          brandFeishu: "飞书",
          brandLark: "Lark",
          connectionStarted: "连接链接已打开",
          connectionReady: "连接准备已完成，正在打开授权链接",
          authStarted: `授权页已打开，${product} 会自动检测授权结果。`,
          authorizationStillPending: `还没有检测到授权完成。请在浏览器完成授权；${product} 会继续自动检测。如果页面没有更新，可点击“我已完成授权”。`,
          permissionTitle: "授权范围",
          permissionDescription:
            "默认只完成基础登录，不会申请任何业务权限。按需在这里勾选要授权的业务域；已连接用户可以重新授权继续追加（scope 会累积）。",
          authDomains: {
            calendar: {
              label: "日历",
              description: "日程、忙闲、日程回复与会议室预定。",
            },
            im: {
              label: "消息",
              description: "收发/回复消息、管理群聊、搜索记录、下载媒体。",
            },
            docs: {
              label: "文档",
              description: "创建、读取、编辑和搜索云文档。",
            },
            drive: {
              label: "云空间",
              description: "上传/下载文件、搜索文档与知识库、管理评论。",
            },
            sheets: {
              label: "电子表格",
              description: "读取、写入、追加、查找和导出电子表格。",
            },
            base: {
              label: "多维表格",
              description: "多维表格的表、字段、记录、视图、仪表盘与工作流。",
            },
            wiki: {
              label: "知识库",
              description: "知识空间、节点与知识库文档。",
            },
            task: {
              label: "任务",
              description: "任务、清单、子任务、评论与提醒。",
            },
            mail: {
              label: "邮件",
              description: "浏览、搜索、阅读、发送、回复、转发与管理草稿。",
            },
            vc: {
              label: "视频会议",
              description: "会议记录、纪要产物与录制。",
            },
            minutes: {
              label: "妙记",
              description: "会议纪要内容与逐字稿。",
            },
            note: {
              label: "笔记",
              description: "会议笔记及相关内容。",
            },
            slides: {
              label: "幻灯片",
              description: "演示文稿与幻灯片内容。",
            },
            markdown: {
              label: "Markdown",
              description: "创建、获取、局部修改和覆盖云盘原生 .md 文件。",
            },
            mindnotes: {
              label: "思维笔记",
              description: "思维笔记内容。",
            },
            contact: {
              label: "通讯录",
              description: "按姓名/邮箱/电话查用户并读取资料。",
            },
            approval: {
              label: "审批",
              description: "查询和处理审批任务、撤销与抄送实例。",
            },
            attendance: {
              label: "考勤",
              description: "查询个人考勤打卡记录。",
            },
            okr: {
              label: "OKR",
              description: "目标、关键结果、对齐、指标与进展。",
            },
            event: {
              label: "实时事件",
              description: "订阅并消费平台实时事件。",
            },
            apps: {
              label: "妙搭应用",
              description: "创建 Spark/妙搭应用、发布站点并管理可见范围。",
            },
            all: {
              label: "全部",
              description:
                "申请 lark-cli 支持的全部业务域权限。仅在不确定缺哪个权限时使用。",
            },
          },
          customScopeLabel: "具体 OAuth scope",
          customScopePlaceholder: "例如 calendar:calendar.event:read",
          customScopeDescription:
            "高级用法：如果错误里给出了缺失 scope，可直接填在这里。例如 calendar:calendar.event:read、calendar:calendar.free_busy:read。",
          openConnectionLinkTitle: "继续完成飞书连接",
          openConnectionLinkDescription:
            "首次连接需要在浏览器里完成一次飞书确认。打开下面的链接按提示完成；完成后回到这里继续授权。",
          openAuthLinkTitle: "在浏览器中完成飞书授权",
          openAuthLinkDescription: `打开下面的链接完成授权。${product} 会持续自动检测，并在授权通过后保存连接状态。`,
          waitingAuthTitle: "等待飞书授权完成",
          waitingAuthDescription: `请在刚打开的浏览器页面完成授权。${product} 会自动更新这里的状态；下方按钮只是兜底操作。`,
          openAuthLink: "打开链接",
          copyAuthLink: "复制链接",
          completeAuth: "我已完成授权",
          continueAuth: "我已完成浏览器确认，继续授权",
          preparingAuthorization: "正在准备授权...",
          completingAuth: "确认中...",
          authExpiresIn: (seconds) => `链接将在约 ${seconds} 秒后过期。`,
          installingTitle: "正在安装官方技能包",
          installingDescription:
            "通常 30 秒内完成，网络较慢时可能需要约 1 分钟。安装完成后会自动刷新状态。",
          installNextTitle: "先安装官方技能包",
          installNextDescription:
            "安装后，/lark-doc、/lark-im、/lark-sheets 等技能会出现在技能索引中。",
          cliNextTitle: "需要安装 Gateway CLI",
          cliNextDescription:
            "技能包已安装，但 Gateway 找不到 lark-cli。管理员重新安装集成会尝试下载 managed Gateway CLI；离线部署可使用内置 @larksuite/cli 的镜像。",
          configuredTitle: "飞书凭证已在本地配置",
          configuredDescription:
            "当前只确认本地存在凭证，尚未向飞书实时验证有效性。重新连接可刷新并实时验证授权。",
          connectedTitle: "飞书授权已实时验证",
          connectedDescription:
            "本次连接流程已向飞书验证当前用户授权。需要刷新授权或追加权限时，可重新连接。",
          authNextTitle: "下一步完成浏览器授权",
          authNextDescription: `点击“连接飞书”后，${product} 会先检查当前状态；未连接或授权过期时会拉起浏览器授权。`,
        },
      },
      skills: {
        provided: "已提供",
        privateSkills: "个人",
        cloneProvided: "复制已提供技能",
        cloneTitle: "创建个人技能副本",
        cloneDescription: "已提供的技能保持不变。副本仅属于你的工作区。",
        sourceLabel: "已提供技能",
        nameLabel: "个人副本名称",
        overrideLabel: "在我的工作区中覆盖已提供的名称",
        overrideDescription:
          "这将更改你的工作区使用的版本，不会更改服务提供方的基础技能。",
        copyAction: "创建个人副本",
        copyPending: "正在创建副本…",
        sourcePlaceholder: "选择已提供技能",
        previewFiles: "此副本包含 {count} 个文件",
        privateOrigin: "{name} 的个人副本",
        baselineOverride: "在你的工作区中覆盖已提供的版本",
        copySuccess: "已创建个人副本。",
        archiveOverride: "允许此归档在我的工作区中覆盖已提供名称",
        title: "技能",
        description: "管理 Agent Skill 配置和启用状态。",
        createSkill: "新建技能",
        emptyTitle: "还没有技能",
        emptyDescription:
          "创建草稿、复制已提供技能，或将 .skill 归档导入你的工作区。",
        emptyButton: "创建你的第一个技能",
        adminRequired: "登录以访问你的个人技能。",
        installAdminRequired: "提供方需要为你的账户启用个人技能管理。",
        installFromFile: "安装 .skill",
        installingArchive: "正在安装…",
        invalidArchive: "请选择扩展名为 .skill 的文件。",
        archiveTooLarge: "技能包大小不能超过 100 MiB。",
        installFailed: "安装技能包失败。",
      },
      notification: {
        title: "通知",
        description: `${product} 只会在窗口不活跃时发送完成通知，特别适合长时间任务：你可以先去做别的事，完成后会收到提醒。`,
        requestPermission: "请求通知权限",
        deniedHint:
          "通知权限已被拒绝。可在浏览器的网站设置中重新开启，以接收完成提醒。",
        testButton: "发送测试通知",
        testTitle: product,
        testBody: "这是一条测试通知。",
        notSupported: "当前浏览器不支持通知功能。",
        disableNotification: "关闭通知",
      },
      account: {
        seats: (count, max) =>
          count > max
            ? `${count.toLocaleString()} 个账号，${max.toLocaleString()} 个席位。每个账号占用一个席位，超过席位数的账号可以继续对话，但无法使用文件。`
            : `已使用 ${max.toLocaleString()} 个席位中的 ${count.toLocaleString()} 个。每个账号占用一个席位。`,
        noSeats: (max, provider) =>
          `全部${max === null ? "" : " " + max.toLocaleString() + " 个"}席位都已使用，无法添加账号。请联系 ${provider} 增加席位，或告知 ${provider} 哪些人的账号已不再需要。`,
        noSeatsSignOn: (max, provider) =>
          `全部 ${max.toLocaleString()} 个席位都已使用，新用户无法登录。请联系 ${provider} 增加席位，或告知 ${provider} 哪些人的账号已不再需要。`,
        profileTitle: "个人信息",
        email: "邮箱",
        role: "角色",
        ssoProvider: "SSO",
        changePasswordTitle: "修改密码",
        changePasswordDescription: "更新你的账号密码。",
        ssoPasswordDescription: "密码由你的 SSO 提供商管理。",
        ssoPasswordMessage: `此账号通过 {provider} 登录，${product} 无法在此管理或修改密码。请前往你的 SSO 提供商账号设置中进行操作。`,
        currentPassword: "当前密码",
        newPassword: "新密码",
        confirmNewPassword: "确认新密码",
        passwordMismatch: "两次输入的新密码不一致",
        passwordTooShort: "密码长度至少为 8 个字符",
        passwordChangedSuccess: "密码修改成功",
        networkError: "网络错误，请重试。",
        updating: "更新中...",
        updatePassword: "修改密码",
        signOut: "退出登录",
        addPersonTitle: "添加成员",
        addPersonDescription:
          "为团队成员创建账号。对方先用一次性密码登录一次，然后设置自己的密码。",
        addPersonEmail: "对方的邮箱地址",
        addPersonSubmit: "添加成员",
        addPersonAdding: "添加中...",
        addPersonAdded: "已添加 {email}。",
        addPersonOneTimePassword: "一次性密码",
        addPersonHandOver:
          "请亲自交给对方。此密码只显示这一次，对方设置自己的密码后即失效。",
        addPersonCopy: "复制",
        addPersonCopied: "已复制",
        addPersonAnother: "再添加一位",
      },
      providerKeys: {
        title: "服务商密钥",
        description:
          "用于 AI 模型和网页搜索的密钥。在此设置的密钥会从下一条消息起替代工作区原有密钥，保存后不会再次显示。你可以选择先测试密钥；保存时不会自动测试。",
        loading: "正在检查服务商密钥…",
        loadError: "无法检查服务商密钥。请重试。",
        retry: "重试",
        groupModels: "AI 模型",
        groupTools: "网页搜索与抓取",
        sourceProduct: "你的密钥",
        sourceEnvironment: "初始配置",
        sourceNone: "无密钥",
        unreadable: "已保存的密钥无法读取",
        unreadableHint:
          "这里保存的密钥无法用本工作区的加密密钥打开，因此在你重新设置之前，该服务商没有密钥。移除它将恢复使用工作区初始配置的密钥（如有）。",
        unreadableHintHostSetting:
          "你保存的密钥仍然存储着，但本工作区目前无法打开它，因此该服务商没有密钥。托管你工作区的人恢复其密钥加密设置后，它会重新生效。移除它会永久丢弃该密钥。",
        refusalNoWrappingKey:
          "目前无法在这里保存密钥：本工作区尚未设置密钥加密。需要由托管你工作区的人进行设置。",
        refusalWrappingKeyInvalid:
          "目前无法在这里保存密钥：本工作区的密钥加密设置无效。需要由托管你工作区的人进行修复。",
        refusalOperatorModelFile:
          "托管你工作区的人负责选择 AI 模型，因此服务商密钥在那里管理，而不是在这里。",
        renderRefused:
          "无法保存此密钥：你的工作区配置不接受它。没有做任何更改。",
        removeConfirm:
          "移除 {provider} 密钥？密钥无法再次显示，以后要使用需要重新粘贴。",
        changed: "{who} 于 {date} 修改",
        set: "添加密钥",
        replace: "替换密钥",
        remove: "移除",
        save: "保存密钥",
        saving: "保存中...",
        cancel: "取消",
        keyLabel: "{provider} 密钥",
        keyPlaceholder: "粘贴密钥",
        test: "测试密钥",
        testing: "正在测试…",
        testHint:
          "可选：向服务商发送一个简短请求，不会保存密钥，服务商可能收取费用。",
        testAccepted: "服务商已接受此密钥。密钥尚未保存。",
        testRejected: "服务商拒绝了此密钥。你可以修改，也可以继续保存。",
        testInconclusive: "服务商未能确认此密钥。你可以重试，也可以继续保存。",
        testTimeout: "服务商未能及时响应。你可以重试，也可以继续保存密钥。",
        testRateLimited: "服务商正在限制请求。稍等后重试，也可以继续保存密钥。",
        testNotTestable: "此服务商不支持在这里测试密钥。你仍可保存密钥。",
        testError: "密钥测试未完成。你可以重试，也可以继续保存。",
        saved: "已保存。你的下一条消息将使用你的 {provider} 密钥。",
        removedToEnvironment:
          "已移除。{provider} 已恢复使用工作区初始配置的密钥。",
        removedToNone: "已移除。{provider} 现在没有密钥。",
        recentChanges: "最近的修改",
        eventAdded: "{who} 添加了 {provider} 密钥",
        eventReplaced: "{who} 替换了 {provider} 密钥",
        eventRemoved: "{who} 移除了 {provider} 密钥",
      },
      acknowledge: {
        emptyTitle: "致谢",
        emptyDescription: "相关的致谢信息会展示在这里。",
      },
    },
    login: {
      signInTitle: "登录你的账号",
      createAccountTitle: "创建新账号",
      email: "邮箱",
      emailPlaceholder: "you@example.com",
      password: "密码",
      passwordPlaceholder: "•••••••",
      rememberMe: "保持登录",
      rememberMeDescription: `下次打开 ${product} 时尽量保持当前会话，仅保存邮箱，不保存密码。`,
      pleaseWait: "请稍候...",
      signIn: "登录",
      createAccount: "创建账号",
      createAdminAccount: "创建管理员账号",
      adminSetupRequiredTitle: "需要先完成管理员初始化",
      adminSetupRequiredDescription: `${product} 需要先创建管理员账号，然后才能创建新的普通账号。`,
      orContinueWith: "或使用以下方式登录",
      ssoHint: "如果你的账号使用单点登录（SSO），请改用下方的选项登录。",
      signOnOnlyDescription:
        "使用你所在组织的账号登录。你的访问权限由组织授予。",
      signOnOnlyNoProvider:
        "本部署通过组织账号登录，但当前没有可用的登录方式。请联系管理员。",
      continueWith: (provider: string) => `使用 ${provider} 登录`,
      noAccountSignUp: "还没有账号？立即注册",
      haveAccountSignIn: "已有账号？立即登录",
      backToHome: "← 返回首页",
      networkError: "网络错误，请重试。",
      serviceUnavailableTitle: "服务暂时不可用",
      serviceUnavailableDescription:
        "网关响应时间过长。请确认服务正在运行，然后重试。",
      retry: "重试",
      authFailed: "身份验证失败。",
      errors: {
        sso_failed: "SSO 登录失败，请重试或使用邮箱登录。",
        sso_cancelled: "SSO 登录已取消。",
        sso_account_exists:
          "该邮箱对应的账号已存在。请使用密码登录或联系管理员。",
        sso_email_unusable:
          "你所在组织的登录未提供可用的邮箱地址。请联系管理员更正。",
        sso_email_taken:
          "该邮箱地址属于本工作区中另一个人的账号。请联系管理员释放该地址。",
        sso_not_allowed: "你的账号不允许使用 SSO 登录。请联系管理员。",
        sso_no_access: "你没有此工作区的访问权限。请联系管理员。",
        sso_access_off: "你对此工作区的访问已被关闭。请联系管理员。",
      },
      signOnOnlyErrors: {
        sso_failed: "登录未完成，请重试。",
        sso_account_exists: "该邮箱已被一个早先的账号占用。请联系管理员清理。",
        sso_email_unusable:
          "你所在组织的登录未提供可用的邮箱地址。请联系管理员更正。",
        sso_email_taken:
          "该邮箱地址属于本工作区中另一个人的账号。请联系管理员释放该地址。",
        sso_no_access: "你没有此工作区的访问权限。请联系管理员。",
        sso_access_off: "你对此工作区的访问已被关闭。请联系管理员。",
      },
    },
    telegramBots: {
      sectionLabel: "Telegram",
      chatName: `${product}`,
      list: {
        title: "您的 Telegram 机器人",
        intro: `${product} 和您的每个自定义智能体都可以在您的 Telegram 中拥有独立的机器人。机器人只回答您。`,
        count: ({ used, limit }) => `${used} / ${limit} 个机器人`,
      },
      kind: {
        custom_agent: "自定义智能体",
      },
      row: {
        lastUsed: ({ time }) => `最后一条消息：${time}`,
        neverUsed: "还没有消息",
        menu: ({ name }) => `${name} 的机器人的更多操作`,
      },
      short: {
        none: "未连接",
        connected: "已连接",
        waiting: "等待按下 Start",
        attention: "需要处理",
        off: "已关闭",
      },
      card: {
        state: ({ state }) => `Telegram：${state}`,
      },
      action: {
        connect: "连接 Telegram",
        open: "在 Telegram 中打开",
        openChat: ({ name }) => `打开与 ${name} 的聊天`,
        newLink: "获取新链接",
        reconnect: "重新连接",
        pasteToken: "粘贴新令牌",
        applyProfile: "重新应用名称和头像",
        otherAccount: "连接其他 Telegram 账号",
        disconnect: "断开连接",
        remove: "移除机器人",
        tryAgain: "重试",
        tryNow: "立即重试",
        thatWasMe: "那是我",
        useHere: "在这里使用",
      },
      offer: {
        chatTitle: `随身带上 ${product}`,
        chatBody: `在 Telegram 上与 ${product} 聊天：在手机上提问、发送文件并获得回答。设置大约需要一分钟。`,
        later: "以后再说",
      },
      sidebar: {
        label: "Telegram",
        connect: "连接",
        connected: ({ count }) => `已连接 ${count} 个`,
        attention: ({ count }) => `${count} 个需要处理`,
      },
      setup: {
        title: ({ name }) => `将 ${name} 连接到 Telegram`,
        intro: ({ name }) =>
          `${name} 会在您的 Telegram 中拥有一个机器人：一个只回答您的聊天。您需要用 Telegram 自己的工具 BotFather 创建这个机器人，大约需要一分钟。`,
        onPhone: "只在手机上使用 Telegram？请在手机上完成此设置。",
        onPhoneLink: "在手机上继续",
        onPhoneQr: "用手机相机扫描并登录，此设置会在手机上打开。",
        before: {
          title: "连接之前",
          body: `这些聊天由 Telegram 传输。您和 ${product} 在那里发送的所有内容，包括文件和语音消息，都会经过 Telegram 的服务器，并保留在您的 Telegram 账号中，直到您删除。机器人聊天并非端到端加密，因此 Telegram 可以读取内容，${product} 无法代您删除。锁屏通知可能显示消息开头；Telegram 和手机都可以隐藏预览。任何能打开您的 Telegram 的人都可以以您的身份给机器人发消息。请使用屏幕锁。如果手机丢失，请在其他设备上结束该会话（Telegram、设置、设备），并在这里断开机器人的连接。`,
          voice: ({ transcriber }) =>
            `语音消息由 ${transcriber} 转换为文字，您需要先在聊天中同意使用该服务。如果服务更换，会再次询问您。${product} 不保留录音。`,
          again: "Telegram 能读取哪些内容，请参阅“连接之前”。",
          show: "查看",
        },
        step1: {
          title: "1. 创建机器人",
          open: "复制用户名并打开 BotFather",
          copied: ({ username }) => `已复制 ${username}`,
          a: "向 BotFather 发送 /newbot。",
          b: ({ botName }) =>
            `BotFather 会询问名称。请输入 ${botName}。打错也没关系：${product} 会设置最终的名称和头像。`,
          c: ({ username }) =>
            `BotFather 会询问用户名。请粘贴您复制的用户名：${username}`,
          copyAgain: "再次复制",
          another: "换一个用户名",
          d: "BotFather 会回复一个令牌，这是一串带冒号的长代码。点按令牌进行复制，然后回到此页面。",
          public: "在 Telegram 上找到此机器人的任何人都能看到它的名称和头像。",
          helpTitle: "在 BotFather 中遇到问题？",
          helpStart: "第一次使用 BotFather？请先按聊天底部的 Start。",
          helpTaken:
            "如果 BotFather 提示用户名已被占用，请在这里换一个再粘贴。",
          helpTooMany:
            "如果 BotFather 提示机器人太多：Telegram 每个账号最多 20 个。请用 /deletebot 删除一个不再使用的机器人。",
          helpCopy:
            "如果点按令牌无法复制，请长按消息并选择“复制”。粘贴 BotFather 的整条消息也可以。",
        },
        step2: {
          title: "2. 粘贴令牌",
          label: "机器人令牌",
          placeholder: "123456789:AAE…",
          paste: "粘贴",
          hint: `${product} 会加密保存令牌，并且不再显示。`,
          checking: "正在向 Telegram 确认…",
          accepted: ({ username }) => `这是 @${username}。正在设置名称和头像…`,
        },
        timezone: {
          ask: "您的浏览器没有提供所在时区。请选择：",
          label: "时区",
        },
        step3: {
          title: "3. 打开聊天并按 Start",
          qr: "或用手机扫描，在手机上打开聊天。",
          why: "您按一次 Start 之后，Telegram 才允许机器人给您发消息。",
          waiting: "正在等待您在 Telegram 中按 Start…",
          hint: "没有反应？请使用此页面上的按钮，而不是 BotFather 发送的链接，然后按聊天底部的 Start。",
          expired: "此链接已过期。",
        },
        done: {
          title: ({ name }) => `${name} 已在 Telegram 上`,
          body: ({ telegramName }) =>
            `已连接为 ${telegramName}。聊天已就绪：请在那里发送第一条消息。`,
          notYou: "不是您的 Telegram 账号？",
          locale: ({ language, timezone }) =>
            `机器人消息使用${language}；时间使用 ${timezone}。`,
          change: "更改",
          profileLater: `名称和头像暂时无法设置。${product} 会再次尝试。`,
          next: ({ nextName }) => `下一个：${nextName}`,
          close: "完成",
        },
      },
      transcriber: {
        openai: "OpenAI",
        gemini: "Google Gemini",
      },
      refusal: {
        format:
          "这不是机器人令牌。令牌形如 123456789:AAE… 请从 BotFather 的最后一条消息中重新复制。",
        rejected:
          "Telegram 不认识此令牌。请从 BotFather 复制最新的令牌并重新粘贴。",
        inUseSelf: ({ other, name }) =>
          `此机器人已连接到 ${other}。一个机器人只服务一个聊天，请为 ${name} 创建另一个机器人。`,
        inUseOther:
          "公司里已有其他人连接了此机器人。请在 BotFather 中创建您自己的机器人。",
        agentLinked: ({ name }) => `您已有 ${name} 的机器人。`,
        webhook:
          "此机器人已连接到其他服务。如果在这里使用，该服务将不再收到此机器人的消息。",
        unreachable: `${product} 无法连接 Telegram。没有保存任何内容。请一分钟后重试。`,
        tooManyCodes:
          "您有五个机器人正在等待按下 Start。请在这些聊天中按 Start，然后重试。",
        agentGone: ({ name }) => `您已无法使用 ${name}，因此无法连接机器人。`,
        ceiling: "您的账号不能连接机器人。请询问管理员。",
        noTimezone: "请选择您的时区，然后重试。",
        gone: "此机器人已不在您的列表中。请刷新页面。",
        failed: "操作没有成功。请重试。",
      },
      state: {
        sectionPaused: "已暂停",
        none: "未连接",
        preparing: "正在设置…",
        waiting: "等待您按下 Start",
        expired: "链接已过期",
        replaced: ({ name }) =>
          `新机器人。请打开聊天并按一次 Start。与 ${name} 的早期会话仍保留在 ${product} 中。`,
        connected: ({ telegramName }) => `已连接为 ${telegramName}`,
        profileStale: "已连接。Telegram 上的名称或头像已过期。",
        held: "在您的访问权限被关闭时已暂停。请重新连接以继续使用此机器人。",
        blocked: ({ name }) =>
          `您在 Telegram 中屏蔽了此机器人，因此 ${name} 无法给您发消息。请打开聊天并解除屏蔽。`,
        conflict:
          "另一个程序正在使用此机器人的令牌，因此此机器人没有回答。如果那不是您，令牌可能已泄露。",
        conflictFix: {
          pasted:
            "要更换令牌，请向 BotFather 发送 /revoke，选择此机器人，然后在这里粘贴新令牌。",
        },
        conflictStopped: "已在 24 小时后停止检查。",
        conflictWebhook: `此机器人已连接到其他服务，因此没有内容到达 ${product}。`,
        afterConflict: {
          chat: ({ time }) =>
            `另一个程序在 ${time} 开始使用此机器人的令牌。现在已停止，机器人恢复回答。在您按下“那是我”或更换令牌之前，此机器人上仍暂停允许访问被阻止的网站。`,
          custom_agent: ({ time }) =>
            `另一个程序在 ${time} 开始使用此机器人的令牌。现在已停止，机器人恢复回答。在您按下“那是我”或更换令牌之前，此机器人上仍暂停允许访问被阻止的网站。`,
        },
        tokenRejected:
          "Telegram 不再接受此机器人的令牌。这通常发生在 BotFather 中执行 /revoke 或 /deletebot 之后。请粘贴新令牌，或移除此机器人。",
        unreachable: `${product} 目前无法连接 Telegram，正在继续尝试。`,
        limited: `Telegram 正在限制此机器人，因此部分消息会延迟到达。已保存的消息会在 Telegram 允许时重试。每条回答也都保留在 ${product} 的会话中。`,
        deliveryPending:
          "消息已保存，正在等待发送。将重试发送，不会启动新的工作。",
        deliveryFailed:
          "一条已保存的消息无法发送。问题解决后，请打开聊天并使用 /help 重试发送。不会启动新的工作。",
        stopped: ({ admin, provider }) =>
          `此机器人因内部错误已停止。请按“立即重试”。如果再次停止，请联系${admin ? ` ${provider} ` : "管理员"}查看。`,
        unreadableHost: ({ provider }) =>
          `此机器人的令牌仍在保存，但目前无法打开，因此机器人没有回答。${provider} 恢复用于加密令牌的设置后，机器人会恢复工作。移除机器人会永久丢弃令牌。`,
        unreadableKey:
          "此机器人保存的令牌无法用这里当前的加密密钥打开，因此机器人没有回答。请粘贴新令牌以恢复机器人。",
        agentGone: ({ name }) => `${name} 已不存在，因此此机器人不再回答。`,
      },
      profile: {
        nameCut: ({ shortened }) =>
          `Telegram 显示的名称最多 64 个字符；此机器人名为 ${shortened}。`,
      },
      token: {
        title: ({ name }) => `为 ${name} 的机器人粘贴新令牌`,
        body: "同一机器人的新令牌不会改变其他内容。其他机器人的令牌会在新连接保存后替换此机器人：之后您需要打开新聊天并按一次 Start。如果保存失败，您当前的机器人会保留。",
        retryPaste:
          "结果尚未确认，或下一步需要令牌。请重新粘贴同一令牌，以安全地继续此请求。",
        completedRemoved: "替换已完成，该机器人后来被移除。机器人列表已刷新。",
        completedChanged:
          "此令牌更改之前已完成。此后机器人又发生了变化。现在显示的是当前状态。",
        replaceFailed: "替换没有成功。您当前的机器人没有变化。",
        savedRetry:
          "令牌已保存，但此机器人无法启动。请在它所在的行选择“立即重试”。",
        same: "这就是此机器人现有的令牌，因此没有任何更改。如需新令牌，请向 BotFather 发送 /revoke，选择此机器人，然后在这里粘贴 BotFather 回复中的令牌。",
      },
      confirm: {
        useHere: {
          title: "在这里使用此机器人？",
          body: "其他服务将不再收到此机器人的消息。",
        },
        thatWasMe: {
          title: "另一个程序是您的吗？",
          body: "只有当您自己用此机器人的令牌运行过其他程序（例如测试或其他工具）时才确认。如果不是，可能有其他人持有令牌：请改为更换令牌。",
          button: "是的，那是我",
        },
        otherAccount: {
          title: "连接其他 Telegram 账号？",
          body: ({ telegramName }) =>
            `此机器人会立即停止接受 ${telegramName} 的操作。不会再向该账号发送新回复，但已发送到 Telegram 的消息仍可能到达。之后您会获得一个新链接，用您想要的 Telegram 账号打开。`,
          button: "断开连接并获取新链接",
        },
        remove: {
          title: ({ name }) => `移除 ${name} 的 Telegram 机器人？`,
          body: ({ name }) =>
            `机器人会立即停止接受操作，令牌也会从 ${product} 中删除。不会再发送新回复；已发送到 Telegram 的消息仍可能到达。您与 ${name} 的会话会保留在 ${product} 中。机器人本身会保留在您的 Telegram 账号中，直到您在 BotFather 中用 /deletebot 删除。`,
          button: "移除机器人",
        },
        lost: {
          title: ({ count }) => `断开全部 ${count} 个机器人？`,
          body: "机器人会立即停止接受操作。不会再向已断开的账号发送新回复；已发送到 Telegram 的消息仍可能到达。准备好后，请在此页面重新连接每个机器人。",
        },
      },
      lost: {
        title: "手机丢失了？",
        body: "立即断开所有机器人的连接。不会删除任何内容，您可以在此页面重新连接每个机器人。",
        button: "断开所有机器人",
      },
      toast: {
        tokenReplaced: ({ name }) =>
          `令牌已更换。${name} 继续在同一聊天中回答。`,
        botReplaced: "机器人已替换。请从它所在的行打开聊天。",
        profileApplied: "名称和头像已应用。",
        thatWasMe: "已确认。此机器人的暂停已结束。",
        usedHere: "完成。此机器人现在在这里回答。",
        disconnected:
          "已断开连接。请用您想要的 Telegram 账号打开聊天并按 Start。",
        removed:
          "已移除。要从 Telegram 删除机器人，请向 BotFather 发送 /deletebot。",
        lost: "所有机器人都已断开连接。",
      },
      unavailable: {
        off: ({ admin, provider }) =>
          `您的公司已关闭 Telegram。请联系${admin ? ` ${provider} ` : "管理员"}开启 Telegram。`,
        offKept: ({ admin, provider }) =>
          `您的公司已关闭 Telegram。您的机器人会保留，在 Telegram 开启后恢复回答。请联系${admin ? ` ${provider} ` : "管理员"}开启 Telegram。`,
        secretMissing: "目前无法添加 Telegram 机器人：缺少用于加密令牌的设置。",
        secretInvalid: "目前无法添加 Telegram 机器人：用于加密令牌的设置无效。",
        hostChanged: ({ provider }) =>
          `Telegram 已暂停：服务已迁移到新服务器，需要 ${provider} 确认此次迁移。您的机器人会保留。`,
        polledElsewhere:
          "Telegram 在这里已暂停，因为多个机器人与另一个程序冲突。您的机器人会保留。",
        unknown: "Telegram 目前不可用。",
        askAdmin: "请询问管理员。",
        next: ({ provider }) => `请联系 ${provider} 解决此问题。`,
        reference: ({ code }) => `支持参考：${code}`,
        limitPerson: ({ count }) =>
          `您有 ${count} 个 Telegram 机器人，已达到这里每人的上限。请移除一个不再使用的机器人后再添加。`,
        limitAll: ({ admin, provider }) =>
          `这里无法再添加 Telegram 机器人：已达到上限。请移除一个不再使用的机器人，或联系${admin ? ` ${provider} ` : "管理员"}提高上限。`,
        loadError: "无法加载您的 Telegram 机器人。请检查网络并刷新。",
      },
      prefs: {
        title: "来自机器人的消息",
        language: {
          label: "机器人消息的语言",
          body: "更改后，每个机器人的描述和命令也会随之更改。",
        },
        timezone: {
          label: "时区",
          body: "在您连接第一个机器人时取自此浏览器。",
        },
        inChat: "在任意机器人中使用 /settings 可更改相同的内容，时区除外。",
        saved: "已保存。",
        failed: "无法保存。请重试。",
      },
      admin: {
        title: "您公司的 Telegram",
        counts: ({ people, bots, polling, failing, held }) =>
          `${people} 人共 ${bots} 个机器人：${polling} 个正常运行，${failing} 个需要所有者处理，${held} 个因访问权限被关闭而暂停。`,
        none: "还没有人连接 Telegram 机器人。",
        limits: ({ perPerson, limit }) =>
          `每人最多可连接 ${perPerson} 个机器人；总共 ${limit} 个。`,
        private:
          "机器人属于个人。除所有者外，没有人能看到机器人的用户名，也没有人能看到令牌。",
        voiceOn: ({ transcriber }) =>
          `语音消息由 ${transcriber} 转换为文字，这是这里已在使用的 AI 服务之一。每个人在发送第一段录音前都要在聊天中同意；如果该服务更换，会再次询问。`,
        voiceOff: ({ provider }) =>
          `这里尚未开启语音消息。当这里设置的某个 AI 模型直接来自 OpenAI 或 Google 时，${provider} 可以开启。`,
      },
      webPage: {
        memory: "打开记忆",
        conversations: "打开会话",
        addAssistant: "添加 AI 助理",
        addEmployee: "添加 AI 员工",
        telegram: "打开 Telegram 设置",
        scheduled: "打开定时任务",
        account: "打开账号",
      },
    },
  };
}

/** The dictionary for a deployment that names no product. */
export const zhCN = createZhCN(DEFAULT_PRODUCT_NAME);
