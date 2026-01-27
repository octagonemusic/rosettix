# 🏛️ Rosettix — Intelligent Polyglot Data Middleware

> **Query multiple databases using natural language — with safety, consistency, and resilience built in.**

[![Java](https://img.shields.io/badge/Java-17-orange)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.2-green)](https://spring.io/projects/spring-boot)
[![AI](https://img.shields.io/badge/Powered_by-Gemini_Pro-blue)](https://deepmind.google/technologies/gemini/)
[![Status](https://img.shields.io/badge/Status-Alpha%20Prototype-yellow)]()

**Rosettix** is an AI-powered middleware framework that sits between client applications and heterogeneous data systems (SQL + NoSQL). It allows natural language querying of databases and ensures safe, consistent distributed writes using an intelligent Saga-based transaction engine.

---

## 🚀 What Does Rosettix Do? (At a Glance)

Rosettix solves two real-world problems in modern backend systems:

### 🔍 1. Natural Language → Database Queries
Non-technical users can query complex databases using plain English:

> *"Show me the top 5 customers by spending in New York"*

Rosettix translates this safely into:
* **SQL queries** for relational databases
* **Structured NoSQL operations** for document stores

All translations are **schema-aware**, **sanitized**, and **injection-safe**.

### ⚡ 2. Reliable Writes Across Multiple Databases
When an operation spans multiple databases (e.g., PostgreSQL + MongoDB), failures can leave data inconsistent. Rosettix uses a **Generic Saga Orchestrator** that:
* Executes distributed write steps safely.
* Automatically generates rollback logic using AI.
* Restores consistency if any step fails — **without 2PC or locking**.

---

## 🧠 Core Ideas Behind Rosettix

| Problem | Rosettix Solution |
| :--- | :--- |
| **Complex DB schemas** | Dynamic schema inspection |
| **SQL injection risks** | Regex guardrails + intent validation |
| **NoSQL ambiguity** | Strict intent-to-method mapping |
| **Distributed failures** | Saga orchestration with compensation |
| **Rigid DB support** | Strategy-based pluggable design |

---

## 🏗️ Architecture Overview

Rosettix follows a **Modular Monolith** architecture, emphasizing extensibility and resilience.

### Key Design Patterns Used
* **Strategy Pattern** — Plug-and-play support for different databases.
* **Saga Pattern (Orchestration)** — Distributed transaction consistency.
* **AI-as-Middleware** — AI used for intent parsing, not blind code generation.

### AI’s Role (Safely Scoped)
AI is used for:
* Natural language intent parsing.
* Generating compensation (rollback) queries.
* Schema-grounded query translation.

**⚠️ Security Note:** AI is never allowed to execute raw scripts directly.

---

## ✨ Key Features

### 🔍 Intelligent Read Pipeline
* Live schema injection from databases.
* SQL generation with sanitization and guardrails.
* NoSQL queries mapped to safe Java method calls.
* Prevents prompt injection and arbitrary execution.

### ⚡ Distributed Write Engine (Saga)
* Accepts dynamic transaction flows (e.g., *Insert → Log → Update → Notify*).
* Generates rollback logic immediately after each successful step.
* Automatically compensates on failure.
* Maintains eventual consistency across systems.

---

## 🛠️ Tech Stack

* **Language:** Java 17
* **Framework:** Spring Boot 3.2
* **AI Integration:** Google Gemini (Vertex AI)
* **Databases:**
    * PostgreSQL (JDBC)
    * MongoDB (Spring Data)
* **Infrastructure:** Docker, Docker Compose
* **Resilience:** Custom exception handling, fault-aware design

---

## 📦 Project Status

**🚧 Alpha Prototype**
* ✅ Core read/write flows implemented
* ✅ Saga orchestration functional
* ✅ Schema-aware AI grounding complete
* 🔮 **Future Work:** Persistence of saga state (currently in-memory)

---

## 🚀 Getting Started

### Prerequisites
* Java 17+
* Docker & Docker Compose
* Google Gemini API Key

### Setup

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/yourusername/rosettix.git](https://github.com/yourusername/rosettix.git)
    cd rosettix
    ```

2.  **Configure environment variables:**
    Create an `application.properties` or `.env` file:
    ```properties
    DB_PASSWORD=your_postgres_password
    MONGO_URI=mongodb://localhost:27017/rosettix
    GOOGLE_API_KEY=your_gemini_api_key
    rosettix.llm.model-name=gemini-2.5-flash
    ```

3.  **Start databases:**
    ```bash
    docker-compose up -d
    ```

4.  **Run the application:**
    ```bash
    ./mvnw spring-boot:run
    ```

---

## 🔌 API Example

### Natural Language Read Query
**POST** `/api/query`

```json
{
  "database": "postgres",
  "question": "Show me the top 5 customers by spending in New York"
}
```
### Rosettix Internal Flow:
1. Inspects live schema
2. Parses intent using AI
3. Generates safe queries
4. Returns structured results

---

## 🎯 Why Rosettix Matters
Rosettix explores how AI can be used responsibly inside backend systems — not as a black box, but as a controlled, schema-aware assistant. It demonstrates:
- Real-world distributed systems thinking.
- Safe AI integration.
- Clean architectural design.
- Production-oriented backend engineering.
