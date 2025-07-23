# Cloudflare R2 对象列表 UI

这是一个基于Flask的Web界面，用于列出Cloudflare R2存储桶中的对象。

## 功能特性

- 🎨 现代化的响应式UI设计
- 🔐 安全的凭据输入（Secret Access Key使用密码字段）
- 📊 清晰的对象列表显示，包含大小和修改时间
- ⚡ 异步请求处理，提供加载状态反馈
- 📱 移动端友好的响应式设计

## 安装和运行

### 方法1：使用启动脚本（推荐）

```bash
cd cloudflare_r2
./start_ui.sh
```

### 方法2：手动安装

```bash
cd cloudflare_r2
pip install -r requirements.txt
python r2_ui.py
```

启动后，在浏览器中访问：http://localhost:5000

## 使用说明

1. **填写配置信息**：
   - **Endpoint URL**: 您的R2端点URL（例如：https://your-account-id.r2.cloudflarestorage.com）
   - **Access Key ID**: 您的访问密钥ID
   - **Secret Access Key**: 您的秘密访问密钥
   - **Bucket 名称**: 要查询的存储桶名称
   - **Prefix (可选)**: 对象前缀，用于过滤特定文件夹的对象

2. **点击"列出对象"按钮**

3. **查看结果**：
   - 成功时会显示对象列表，包含对象名称、大小和最后修改时间
   - 错误时会显示详细的错误信息

## 配置参数说明

- **Endpoint URL**: Cloudflare R2的端点URL，格式为 `https://{account-id}.r2.cloudflarestorage.com`
- **Access Key ID**: 在Cloudflare R2控制台创建的API令牌的访问密钥ID
- **Secret Access Key**: 对应的秘密访问密钥
- **Bucket**: 要查询的存储桶名称
- **Prefix**: 可选参数，用于只列出特定前缀的对象（例如：`folder/` 只列出folder文件夹下的对象）

## 安全注意事项

- 所有凭据信息仅在内存中处理，不会保存到磁盘
- 建议在生产环境中使用HTTPS
- 请妥善保管您的API凭据

## 文件结构

```
cloudflare_r2/
├── r2_ui.py              # Flask应用主文件
├── templates/
│   └── index.html        # UI模板文件
├── requirements.txt       # Python依赖
├── start_ui.sh          # 启动脚本
└── README.md            # 说明文档
```

## 故障排除

1. **连接错误**: 检查Endpoint URL是否正确
2. **认证错误**: 验证Access Key ID和Secret Access Key是否正确
3. **权限错误**: 确保API令牌有足够的权限访问指定的存储桶
4. **存储桶不存在**: 确认存储桶名称拼写正确 