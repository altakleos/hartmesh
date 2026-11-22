import { useI18n } from "@/core/i18n/hooks";

export function useBrowserCopy() {
  const { locale } = useI18n();
  const zh = locale === "zh-CN";
  return {
    uploads: zh ? "上传文件" : "Uploads",
    outputs: zh ? "输出文件" : "Outputs",
    upload: zh ? "上传" : "Upload",
    locations: zh ? "位置" : "Locations",
    path: zh ? "完整路径" : "Full path",
    skip: zh ? "跳至文件" : "Skip to files",
    details: zh ? "详情" : "Details",
    audience: zh ? "可见范围" : "Audience",
    more: zh ? "加载更多" : "Load more",
    locationsFilter: zh ? "筛选已加载的位置" : "Filter loaded locations",
    myAudience: zh ? "仅你可见" : "Only you",
    companyAudience: zh ? "公司中的所有人" : "Everyone at the company",
    disclose: zh
      ? "发布副本到公司共享区。公司中的所有人都可以打开，也可以通过其 AI 助理和员工访问。"
      : "Publish a copy to Company shared. Everyone at the company can open it, including through its AI assistants and employees.",
    agentAudience: zh
      ? "可以查看其全部工作的人员可以浏览文件。可以与其聊天的人员也可以询问文件内容。"
      : "People who can see all its work can browse these files. Anyone who can chat with it can ask about them.",
    folderFilter: zh ? "筛选当前文件夹" : "Filter this folder",
    limited: zh ? "搜索已加载的文件" : "Search loaded files",
    search: (s: string) => (zh ? `搜索${s}` : `Search ${s}`),
    denied: zh
      ? "你已无法访问此位置。"
      : "You no longer have access to this location.",
    folderScanLimit: zh
      ? "扫描达到上限，尚未找到可见条目。"
      : "The scan reached its limit before finding a visible entry.",
    noMatch: zh
      ? "已加载的文件中没有匹配项。"
      : "No matches among the loaded files.",
    actions: (s: string) => (zh ? `${s} 的更多操作` : `More actions for ${s}`),
    parent: (s: string) =>
      zh ? `打开所在文件夹：${s}` : `Open containing folder: ${s}`,
    missing: zh ? "此文件夹已不可用。" : "This folder is no longer available.",
    personal: zh ? "我的文件" : "My files",
    shared: zh ? "公司共享" : "Company shared",
    partial: (n: number) =>
      zh
        ? `这里只能查看前 ${n} 个条目。`
        : `Only the first ${n} entries are available here.`,
    downloadDenied: (file: string) =>
      zh ? `无法再访问 ${file}。` : `You no longer have access to ${file}.`,
    downloadFailed: (file: string) =>
      zh
        ? `无法下载 ${file}。请重试。`
        : `Could not download ${file}. Try again.`,
    published: zh ? "发布时间" : "Published",
    matches: (n: number) => (zh ? `${n} 个匹配项` : `${n} matching entries`),
    empty: zh ? "此文件夹为空。" : "This folder is empty.",
    drop: (s: string) => (zh ? `上传到 ${s}` : `Upload to ${s}`),
  };
}
