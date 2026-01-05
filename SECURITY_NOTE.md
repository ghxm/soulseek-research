# Security Notes

## Encryption Key Protection

### ✅ What's Protected

The encryption key used for username hashing is **never stored in git**:

1. **terraform.tfvars** (contains the real key) is in `.gitignore`
2. Setup scripts use Terraform variables, not hardcoded values
3. GitHub Actions doesn't need the key (only reads pre-hashed data)

### 🔒 Username Privacy

Usernames are **hashed, not encrypted**:
- **SHA-256 one-way hash** with salt (encryption key)
- **Cannot be decrypted** - irreversible
- Same username always produces same hash (allows tracking repeat users)
- Without the encryption key, you can't determine if a specific username is in the database

### 📊 Public Dashboard Safety

The GitHub Pages dashboard is safe to publish:
- Shows only pre-hashed usernames from database
- No raw usernames ever exposed
- No encryption key needed for display
- All user data anonymized

### 🔑 Where the Encryption Key is Used

The encryption key (`ENCRYPTION_KEY`) is only needed during **data collection**:

1. **Client-side** (client.py): Hashes usernames as they arrive
2. **Database**: Stores only the hashed values
3. **Dashboard**: Reads pre-hashed values (no key needed)

### ⚠️ If the Encryption Key Leaks

If someone gets your encryption key, they could:
- Hash a username and search for it in your database
- Determine if a specific user has made searches

They **cannot**:
- Reverse any hashes to get original usernames
- Decrypt existing database entries

**Mitigation**: Rotate the encryption key and re-hash historical data if needed.

### 🛡️ Best Practices

1. **Never commit terraform.tfvars** (already in .gitignore)
2. **Use a strong, random encryption key** (not a simple string)
3. **Don't share the encryption key** outside secure channels
4. **Rotate periodically** if you suspect compromise
5. **Use different keys** for dev/staging/production environments

### 🔄 Changing the Encryption Key

If you need to change it:

1. Update `terraform.tfvars`:
   ```hcl
   encryption_key = "new_random_key_here"
   ```

2. Re-deploy infrastructure:
   ```bash
   make deploy
   ```

3. **Note**: Historical data will have different hashes, so user continuity is lost across key changes.

### ✅ Current Security Status

- ✅ Encryption key not in git repository
- ✅ Key passed via Terraform (sensitive variable)
- ✅ Usernames irreversibly hashed before storage
- ✅ GitHub Pages dashboard safe to publish
- ✅ No credentials in public documentation
