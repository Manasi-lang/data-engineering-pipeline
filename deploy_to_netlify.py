#!/usr/bin/env python3
"""
One-Click Netlify Deployment
Deploys dashboard to Netlify for instant public URL
"""

import os
import subprocess
import webbrowser
from pathlib import Path

def deploy_with_netlify_cli():
    """Deploy using Netlify CLI (if available)"""
    try:
        # Check if netlify CLI is installed
        result = subprocess.run(['netlify', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("🚀 Netlify CLI found, deploying...")
            
            # Navigate to dashboard directory
            dashboard_path = Path.home() / "Desktop" / "Cloud_Dashboard"
            os.chdir(dashboard_path)
            
            # Deploy to Netlify
            result = subprocess.run(['netlify', 'deploy', '--prod', '--dir', '.'], 
                               capture_output=True, text=True)
            
            if result.returncode == 0:
                # Extract URL from output
                output = result.stdout
                for line in output.split('\n'):
                    if 'Website URL:' in line:
                        url = line.split('Website URL:')[1].strip()
                        print(f"✅ Dashboard deployed successfully!")
                        print(f"🌐 Public URL: {url}")
                        return url
            
            print("❌ Netlify deployment failed")
            print(f"Error: {result.stderr}")
            
    except FileNotFoundError:
        print("📦 Netlify CLI not found")
        return None

def deploy_manual_instructions():
    """Provide manual deployment instructions"""
    dashboard_path = Path.home() / "Desktop" / "Cloud_Dashboard"
    
    print("📋 MANUAL DEPLOYMENT INSTRUCTIONS:")
    print("=" * 50)
    print(f"📁 Dashboard folder: {dashboard_path}")
    print()
    print("1️⃣ EASIEST - NETLIFY DRAG & DROP:")
    print("   🌐 Open: https://app.netlify.com/drop")
    print("   📁 Drag the Cloud_Dashboard folder to the drop zone")
    print("   ⚡ Get instant URL: https://[random-name].netlify.app")
    print("   💰 100% free forever")
    print()
    
    print("2️⃣ PROFESSIONAL - GITHUB PAGES:")
    print("   📁 Create new repository at: https://github.com/new")
    print("   📤 Upload Cloud_Dashboard folder")
    print("   ⚙️  Go to Settings > Pages")
    print("   🌐 Enable GitHub Pages")
    print("   🔗 URL: https://[username].github.io/[repo]/")
    print("   💰 Free with GitHub account")
    print()
    
    print("3️⃣ MODERN - VERCEL:")
    print("   🌐 Open: https://vercel.com")
    print("   📤 Click 'New Project' > 'Import Git Repository'")
    print("   📁 Or drag & drop Cloud_Dashboard folder")
    print("   ⚡ Automatic deployments")
    print("   🌐 URL: https://[project].vercel.app")
    print("   💰 Free tier available")
    print()
    
    print("🎯 RECOMMENDED: Use Netlify Drop for instant URL!")
    print("⏱️  Takes 2 minutes total!")

def create_deployment_script():
    """Create a simple deployment script"""
    script_content = '''#!/bin/bash
echo "🚀 Quick Netlify Deployment"
echo "========================"

# Navigate to dashboard
cd ~/Desktop/Cloud_Dashboard

echo "📁 Opening Netlify Drop..."
open "https://app.netlify.com/drop"

echo "🌐 Once uploaded, you'll get a public URL instantly!"
echo "💡 Drag this folder to the Netlify drop zone:"
pwd
'''
    
    script_path = Path.home() / "Desktop" / "Cloud_Dashboard" / "deploy.sh"
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    os.chmod(script_path, 0o755)
    return script_path

def main():
    """Main deployment function"""
    print("🌐 One-Click Cloud Deployment")
    print("=" * 40)
    
    # Try automatic deployment
    url = deploy_with_netlify_cli()
    
    if not url:
        # Create deployment script
        script_path = create_deployment_script()
        
        # Provide manual instructions
        deploy_manual_instructions()
        
        # Open Netlify drop
        try:
            webbrowser.open("https://app.netlify.com/drop")
            print("🌐 Netlify Drop opened in your browser!")
        except:
            print("💡 Open manually: https://app.netlify.com/drop")
        
        print(f"\n📋 Quick script created: {script_path}")
        print("💡 Run it to navigate to dashboard folder and open Netlify")
    
    return url is not None

if __name__ == "__main__":
    success = main()
    if not success:
        print("\n🎯 Manual deployment required - see instructions above!")
