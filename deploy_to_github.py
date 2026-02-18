#!/usr/bin/env python3
"""
GitHub Pages Deployment
Deploys dashboard to GitHub Pages for professional URL
"""

import subprocess
import webbrowser
from pathlib import Path

def create_github_repo():
    """Create GitHub repository and deploy"""
    dashboard_path = Path.home() / "Desktop" / "Cloud_Dashboard"
    
    print("🚀 GitHub Pages Deployment")
    print("=" * 40)
    
    print(f"📁 Dashboard folder: {dashboard_path}")
    print()
    
    print("📋 STEP-BY-STEP DEPLOYMENT:")
    print("=" * 30)
    
    print("1️⃣ CREATE GITHUB REPOSITORY:")
    print("   🌐 Open: https://github.com/new")
    print("   📝 Repository name: data-pipeline-dashboard")
    print("   📋 Description: Interactive Data Engineering Dashboard")
    print("   ☑️  Public: YES")
    print("   ☑️  Add README: NO")
    print("   🚀 Click 'Create repository'")
    print()
    
    print("2️⃣ UPLOAD DASHBOARD FILES:")
    print("   📁 Click 'uploading an existing file'")
    print("   📤 Select all files in Cloud_Dashboard folder")
    print("   📝 Commit message: 'Add interactive dashboard'")
    print("   🚀 Click 'Commit changes'")
    print()
    
    print("3️⃣ ENABLE GITHUB PAGES:")
    print("   ⚙️  Go to Settings > Pages")
    print("   📂 Source: Deploy from a branch")
    print("   🌿 Branch: main")
    print("   📁 Folder: / (root)")
    print("   🚀 Click 'Save'")
    print()
    
    print("4️⃣ GET YOUR PUBLIC URL:")
    print("   ⏳ Wait 2-3 minutes for deployment")
    print("   🌐 URL: https://[your-username].github.io/data-pipeline-dashboard/")
    print("   🎯 Professional URL with your GitHub username!")
    print()
    
    print("🎯 ADVANTAGES OF GITHUB PAGES:")
    print("   ✅ Free forever")
    print("   ✅ Professional URL")
    print("   ✅ HTTPS enabled")
    print("   ✅ Custom domain support")
    print("   ✅ Version control")
    print("   ✅ Automatic deployments")
    print()
    
    # Open GitHub in browser
    try:
        webbrowser.open("https://github.com/new")
        print("🌐 GitHub opened in your browser!")
    except:
        print("💡 Open manually: https://github.com/new")
    
    return True

def create_deployment_guide():
    """Create a detailed deployment guide"""
    guide_content = """# 🚀 GitHub Pages Deployment Guide

## Quick Steps (2 minutes)

### 1. Create Repository
1. Go to https://github.com/new
2. Repository name: `data-pipeline-dashboard`
3. Description: `Interactive Data Engineering Dashboard`
4. Make it **Public**
5. Click **Create repository**

### 2. Upload Files
1. Click **uploading an existing file**
2. Drag all files from `Cloud_Dashboard` folder
3. Commit message: `Add interactive dashboard`
4. Click **Commit changes**

### 3. Enable GitHub Pages
1. Go to **Settings** > **Pages**
2. Source: **Deploy from a branch**
3. Branch: **main**
4. Folder: **/(root)**
5. Click **Save**

### 4. Get Your URL
Wait 2-3 minutes, then visit:
```
https://[your-username].github.io/data-pipeline-dashboard/
```

## 🎯 Your Dashboard Features
- 📊 Real-time metrics and charts
- 📈 Interactive data visualizations
- 🌐 Responsive design
- 📱 Mobile-friendly
- ⚡ Fast loading
- 🔒 HTTPS security

## 🌐 Alternative Platforms
- **Netlify**: https://app.netlify.com/drop (Instant)
- **Vercel**: https://vercel.com (Modern)
- **GitHub Pages**: https://pages.github.com (Professional)

---
🚀 Built with Python, Pandas, and Chart.js
"""
    
    guide_path = Path.home() / "Desktop" / "Cloud_Dashboard" / "DEPLOYMENT_GUIDE.md"
    with open(guide_path, 'w') as f:
        f.write(guide_content)
    
    print(f"📋 Deployment guide created: {guide_path}")
    return guide_path

def main():
    """Main deployment function"""
    # Create deployment guide
    create_github_repo()
    create_deployment_guide()
    
    print("\n✅ Ready for GitHub Pages deployment!")
    print("🌐 Your dashboard will have a professional URL")
    print("📋 Follow the steps above for instant deployment")

if __name__ == "__main__":
    main()
