# Branch Protection Configuration

This document outlines the recommended branch protection rules for the `main` branch to ensure code quality and prevent broken builds from being merged.

## Required Branch Protection Rules

### For the `main` branch:

1. **Require status checks to pass before merging**
   - Enable: "Require status checks to pass before merging"
   - Required status checks:
     - `build` (from the CI workflow)

2. **Require pull request reviews before merging**
   - Enable: "Require pull request reviews before merging"
   - Required number of reviewers: 1 (adjust as needed)

3. **Require conversation resolution before merging**
   - Enable: "Require conversation resolution before merging"

4. **Require branches to be up to date before merging**
   - Enable: "Require branches to be up to date before merging"

5. **Include administrators**
   - Enable: "Include administrators" (recommended for consistency)

## How to Configure

### Via GitHub UI:
1. Go to your repository on GitHub
2. Navigate to Settings → Branches
3. Click "Add rule" for the `main` branch
4. Configure the settings as outlined above

### Via GitHub CLI:
```bash
# Enable branch protection with required status checks
gh api repos/:owner/:repo/branches/main/protection \
  --method PUT \
  --field required_status_checks='{"strict":true,"contexts":["build"]}' \
  --field enforce_admins=true \
  --field required_pull_request_reviews='{"required_approving_review_count":1,"dismiss_stale_reviews":true}' \
  --field restrictions=null
```

### Via GitHub API:
```bash
curl -X PUT \
  -H "Authorization: token YOUR_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/OWNER/REPO/branches/main/protection \
  -d '{
    "required_status_checks": {
      "strict": true,
      "contexts": ["build"]
    },
    "enforce_admins": true,
    "required_pull_request_reviews": {
      "required_approving_review_count": 1,
      "dismiss_stale_reviews": true
    },
    "restrictions": null
  }'
```

## Result

With these settings:
- Pull requests must pass the `build` status check (from the CI workflow)
- Pull requests must be reviewed and approved
- Pull requests must be up to date with the main branch
- All conversations must be resolved before merging
- No direct pushes to main are allowed (except for repository administrators, if configured)
