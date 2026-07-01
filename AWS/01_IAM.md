# AWS Data Engineering — Study Pack
## Tier 1 · Topic 1: IAM (Identity and Access Management)

> For a data engineer coming from Apache + GCP. Every AWS data service (Glue, Redshift,
> EMR, Lambda, Kinesis) runs *as an IAM role*. Get this right and everything downstream works.

---

## 1. What & Why
IAM is AWS's **authentication + authorization** layer. Every API call answers two questions:
*Who are you?* (authn) and *Are you allowed?* (authz). Nothing happens in AWS without an IAM decision.

**Mental shift from GCP:** GCP leans on the Org → Folder → Project hierarchy for inheritance.
AWS IAM is **JSON-policy driven** and far more explicit — you write policies and attach them to identities.

---

## 2. Core Building Blocks

### Principals (the "who")
- **Root user** — account owner, god-mode. Set up, enable MFA, then *never use again*.
- **IAM Users** — long-lived human/app identities (passwords, access keys). Avoid in modern setups.
- **IAM Roles** — identity with **no permanent credentials**, *assumed* temporarily. **The DE workhorse.**
  Services assume roles: Glue → Glue execution role, Lambda → Lambda execution role,
  EMR → service role + EC2 instance profile, Redshift → IAM role for COPY/UNLOAD.
- **IAM Groups** — collections of users for easier policy management (not assumable).

> **Key insight:** Services don't have permissions themselves. You make a role, attach policies,
> and tell the service "run as this role."

### Policies (the "what") — JSON documents
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowReadDataBucket",
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::my-data-lake",
      "arn:aws:s3:::my-data-lake/*"
    ]
  }]
}
```
Each statement: **Effect** (Allow/Deny) · **Action** (API calls) · **Resource** (ARNs) · **Condition** (optional).

### ARN — Amazon Resource Name
Globally unique resource ID. Format: `arn:aws:service:region:account-id:resource`.

---

## 3. Two Policy Types (the #1 interview trap)
- **Identity-based** — attached to user/group/role. "*This identity* can do X on Y."
- **Resource-based** — attached to the resource (S3 bucket policy, SQS/Lambda policy). "*This resource*
  allows these principals to do X." Enables **cross-account** access and names the principal explicitly.

### Evaluation logic (memorize)
1. **Explicit Deny** anywhere → **denied**. Deny always wins.
2. Else an **explicit Allow** in any applicable policy → **allowed**.
3. Else → **implicit deny** (default).

With SCPs + permission boundaries: *every* layer that applies must allow, and *no* layer may deny.

---

## 4. AssumeRole / STS
- **STS** (Security Token Service) issues **temporary credentials** (access key + secret + session token, expiring).
- A role has two parts:
  - **Trust policy** (resource-based, on the role): *who may assume it.*
  - **Permissions policies** (identity-based): *what it can do once assumed.*

```json
// Trust policy: lets the Glue service assume this role
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```
> **Gotcha:** AccessDenied with correct permissions? **Check the trust policy first.**

---

## 5. GCP / Apache Mapping
| Concept | GCP | AWS |
|---|---|---|
| Permission bundle | Predefined/Custom Role | Managed/Customer-managed Policy |
| Identity for a service | Service Account | IAM Role |
| Temp credential | SA impersonation / metadata token | STS AssumeRole |
| Cross grant | IAM binding on resource | Resource-based policy / role trust |
| Guardrails | Org Policy | SCP (Organizations) |
| "Who am I" | `gcloud auth list` | `aws sts get-caller-identity` |

> GCP Service Accounts are durable identities *with their own keys* — closer to an AWS **user** in that
> respect but used like an AWS **role**. Don't over-map; just remember "service needs perms → make a role."

---

## 6. Production Best Practices
- **Least privilege** — scope actions and resources tightly. No `"*"` on `"*"` in prod.
- **Roles over users** — humans log in via **IAM Identity Center (SSO)** federated to Okta/AD → temporary creds.
- **No access keys in code** — use execution roles / instance profiles. Hardcoded key = incident.
- **One role per workload** — blast-radius containment.
- **Permission boundaries** — cap what self-service teams can grant.
- **ABAC (tag-based access)** — grant by resource tags instead of enumerating ARNs; scales better.
- **CloudTrail** logs every API call; **IAM Access Analyzer** finds external access + generates least-priv policies.

---

## 7. Common Mistakes & Limits
- Confusing identity-based vs resource-based → cross-account fails mysteriously.
- Forgetting the **trust policy** → service can't assume the role.
- Over-broad `s3:*` on `*` — most common audit finding.
- Assuming an Allow overrides a Deny (it never does).
- Limits: managed policy ~6,144 chars; ~10 managed policies/role by default; inline policy size caps.
- **Eventual consistency** — IAM changes take seconds to propagate ("worked on retry").

---

## ⭐ Key Takeaways
1. IAM gates **every** AWS API call. **Deny > Allow > implicit deny.**
2. **Roles are the DE workhorse** — assumed, temporary, no standing secrets (via STS).
3. **Role = trust policy (who can assume) + permissions policy (what it can do).**
4. Identity-based vs resource-based; resource-based enables cross-account.
5. Least privilege, roles not users, SSO for humans, no hardcoded keys, audit with CloudTrail.
6. Every data service runs as a role — internalize the per-service role pattern.

---

# AWS Data Engineering — Study Pack
## Tier 1 · Topic 1: IAM — Model Answers (Q&A)

1. What's the difference between an IAM user and an IAM role, and which should a Glue job use?
### Q1. IAM user vs role; which for a Glue job?
**User** = durable identity with **long-lived credentials** (password, access keys), for a human/legacy app.
**Role** = **no permanent credentials**, *assumed* on demand; STS issues temporary, auto-expiring creds.

A Glue job uses a **role** (the Glue execution role). Trust policy lets `glue.amazonaws.com` assume it;
permissions policies grant S3 / Glue Catalog / CloudWatch. Never bake access keys into a service.

---
2. In plain terms, what does an ARN identify?
### Q2. What does an ARN identify?
A **single AWS resource**, uniquely, across all of AWS. Encodes service, region, account, resource path:
`arn:aws:s3:::my-bucket`, `arn:aws:lambda:us-east-1:123456789012:function:my-fn`.
Policies use ARNs to scope *exactly which* resources an action applies to.

---
3. A Lambda function needs to read from an S3 bucket. Walk me through every IAM piece that must be in place for this to work.
### Q3. Every IAM piece for a Lambda to read S3
1. **Lambda execution role** (what Lambda runs as).
2. **Trust policy** allowing `lambda.amazonaws.com` to `sts:AssumeRole`.
3. **Permissions policy**: `s3:GetObject` (+ `s3:ListBucket` if listing) on bucket ARN + `/*` objects ARN.
4. **No conflicting Deny** (SCP / boundary / bucket policy).
5. If **SSE-KMS** encrypted: `kms:Decrypt` on the key **and** the KMS **key policy** must allow the role. *(Most-missed.)*
6. If **cross-account**: bucket policy must also allow the role's account/ARN.

---
4. You attach a policy that allows `s3:GetObject`, but the call still fails with AccessDenied. Give me three distinct possible causes.
### Q4. Three causes of AccessDenied despite an Allow
- **Explicit Deny** elsewhere (SCP, permission boundary, bucket policy) overriding the Allow.
- **Resource ARN mismatch** — allowed `:::bucket` but `GetObject` needs `:::bucket/*`. (Very common.)
- **KMS** — SSE-KMS object, principal lacks `kms:Decrypt` (or key policy doesn't grant it). S3 reports AccessDenied.
- *(Also valid: cross-account bucket policy missing; boundary too narrow; wrong principal; IAM propagation delay.)*

---
5. Team A's account owns an S3 bucket. Team B's account has a Glue job that must read it. Describe two different ways to grant this access, and which you'd prefer in production.
### Q5. Cross-account S3 (Team A bucket, Team B Glue job)
**Option A — Bucket policy (resource-based):** Team A's bucket policy allows Team B's Glue role ARN to
`s3:GetObject`/`s3:ListBucket`; Team B's role has a matching identity policy. Both sides must allow.

**Option B — Cross-account role assumption:** Team A creates a role (with S3 access) whose trust policy lets
Team B's Glue role assume it; Team B's job calls `sts:AssumeRole` and reads as a Team-A principal.

**Prod preference:** Bucket policy for simple S3-read (fewer hops, no extra assume, Glue reads directly).
Use assume-role when Team B needs broad access to many Team-A resources or A wants one auditable access role.
Governed-lake answer: **Lake Formation cross-account grants**.

---
6.Explain the full IAM policy evaluation order when an SCP, an identity-based policy, and a resource-based policy all apply to one request. What's the deciding rule if they disagree?
### Q6. Evaluation order: SCP + identity + resource policy
1. **Any explicit Deny → denied** (any layer). Deny always wins.
2. **SCP must Allow** (Organizations) — a ceiling; grants nothing, only permits/forbids.
3. **Permission boundary must Allow** (if attached) — same ceiling logic.
4. **Same-account:** Allow in identity **or** resource policy suffices.
   **Cross-account:** identity **and** resource policy must both Allow.
5. No Allow → **implicit deny**.

Disagreement rule: **Deny wins**; else every ceiling must permit AND required Allow(s) must be present.

---
7. Your security team wants application teams to self-serve IAM roles but mandates that no role they create can ever grant `s3:DeleteBucket` or escape a permission ceiling. Which IAM feature(s) achieve this, and how?
### Q7. Self-serve roles, capped (no `s3:DeleteBucket`, no escape)
**Permission boundaries** — mandate a boundary on every team-created role. Effective perms =
**intersection** of permissions policies ∩ boundary. Even `s3:*` is capped if boundary omits/denies
`s3:DeleteBucket`. Boundaries also block privilege escalation.

**SCPs (Organizations)** — account/OU-wide **explicit Deny** of `s3:DeleteBucket` (and dangerous IAM actions).
The hard floor for everyone, including newly created roles.

**Together:** SCP = org-wide hard Deny; boundary = per-role ceiling + anti-escalation; an IAM policy requires
`iam:CreateRole` to attach the mandated boundary (`Condition` on `iam:PermissionsBoundary`). Self-serve enabled,
forbidden actions structurally impossible.

---
*Companion to: 01_AWS_IAM_Notes.md · Next topic: 02_AWS_S3_Notes.md*