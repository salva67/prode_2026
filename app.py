from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import os
import csv
import string
import random


DB_PATH = "prode.db"
FIXTURE_CSV = "fixture_2026.csv"

app = Flask(__name__)


# -----------------------------
# Conexión y setup de la base
# -----------------------------
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_fixture_from_csv():
    """
    Lee el fixture desde fixture_2026.csv.
    Formato esperado del CSV:
    group_name,stage,kickoff,home_team,away_team
    """
    matches = []
    if not os.path.exists(FIXTURE_CSV):
        print(f"⚠️ No se encontró {FIXTURE_CSV}, se cargarán partidos de ejemplo.")
        matches = [
            ("Group A", "Group Stage", "2026-06-11 16:00", "Mexico", "South Africa", None, None),
            ("Group A", "Group Stage", "2026-06-11 23:00", "South Korea", "TBD", None, None),
            ("Group J", "Group Stage", "2026-06-16 22:00", "Argentina", "Algeria", None, None),
        ]
        return matches

    with open(FIXTURE_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            group_name = row.get("group_name", "").strip() or None
            stage = row.get("stage", "").strip() or "Group Stage"
            kickoff = row["kickoff"].strip()
            home_team = row["home_team"].strip()
            away_team = row["away_team"].strip()
            if home_team and away_team and kickoff:
                matches.append((group_name, stage, kickoff, home_team, away_team, None, None))

    return matches


def init_db():
    """Crea la base y carga el fixture la primera vez."""
    if os.path.exists(DB_PATH):
        return

    conn = get_db_connection()
    cur = conn.cursor()

    # Usuarios (simple: nombre único)
    cur.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Partidos
    cur.execute(
        """
        CREATE TABLE matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT,
            stage TEXT,
            kickoff TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_score INTEGER,
            away_score INTEGER
        );
        """
    )

    # Pronósticos
    cur.execute(
        """
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            home_pred INTEGER NOT NULL,
            away_pred INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(match_id, user_id),
            FOREIGN KEY(match_id) REFERENCES matches(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )

    # Cargar fixture
    matches = load_fixture_from_csv()
    cur.executemany(
        """
        INSERT INTO matches (group_name, stage, kickoff, home_team, away_team, home_score, away_score)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        matches,
    )

    conn.commit()
    conn.close()
    print("✅ Base creada y fixture cargado.")


def ensure_pool_tables():
    """
    Crea las tablas de ligas (pools) si no existen.
    Se llama siempre al arrancar la app, incluso si la DB ya existía.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Tabla de ligas
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Miembros de las ligas
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pool_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pool_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT DEFAULT 'member',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(pool_id, user_id),
            FOREIGN KEY(pool_id) REFERENCES pools(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )

    conn.commit()
    conn.close()

def generate_pool_code(length: int = 6) -> str:
    """
    Genera un código único de liga, ej: 'A7K3QZ'.
    """
    chars = string.ascii_uppercase + string.digits
    conn = get_db_connection()
    cur = conn.cursor()

    while True:
        code = "".join(random.choices(chars, k=length))
        row = cur.execute("SELECT id FROM pools WHERE code = ?;", (code,)).fetchone()
        if row is None:
            conn.close()
            return code


def generate_pool_code(length: int = 6) -> str:
    """
    Genera un código único de liga, ej: 'A7K3QZ'.
    """
    chars = string.ascii_uppercase + string.digits
    conn = get_db_connection()
    cur = conn.cursor()

    while True:
        code = "".join(random.choices(chars, k=length))
        row = cur.execute("SELECT id FROM pools WHERE code = ?;", (code,)).fetchone()
        if row is None:
            conn.close()
            return code

# -----------------------------
# Lógica de puntos del prode
# -----------------------------
def compute_points(home_pred, away_pred, home_score, away_score):
    """
    Regla de puntos:
      - Partido sin resultado (home_score/away_score None) → None (no se cuenta).
      - 5 puntos: resultado exacto (marcador exacto).
      - 4 puntos: acierta ganador/empate y diferencia de gol, pero no marcador exacto.
      - 3 puntos: acierta solo ganador/empate.
      - 0 puntos: resto.
    """
    if home_score is None or away_score is None:
        return None

    try:
        hp = int(home_pred)
        ap = int(away_pred)
        hs = int(home_score)
        as_ = int(away_score)
    except (TypeError, ValueError):
        return 0

    # Resultado real
    if hs > as_:
        real = "H"
    elif hs < as_:
        real = "A"
    else:
        real = "D"

    # Resultado pronosticado
    if hp > ap:
        pred = "H"
    elif hp < ap:
        pred = "A"
    else:
        pred = "D"

    # Exacto
    if hp == hs and ap == as_:
        return 5

    # Ganador/empate correcto
    if pred == real:
        # Diferencia correcta
        if (hs - as_) == (hp - ap):
            return 4
        return 3

    return 0


# -----------------------------
# Rutas
# -----------------------------

@app.route("/", methods=["GET", "POST"])
def index():
    """
    Pantalla de inicio:
      - Listado de usuarios existentes para entrar.
      - Form para crear usuario nuevo.
      - Dashboard con stats básicas.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if name:
            try:
                cur.execute("INSERT INTO users (name) VALUES (?);", (name,))
                conn.commit()
            except sqlite3.IntegrityError:
                # Si ya existe el nombre, lo ignoramos y usamos el existente
                pass

            user = cur.execute("SELECT id, name FROM users WHERE name = ?;", (name,)).fetchone()
            conn.close()
            if user:
                return redirect(url_for("fixture", user_id=user["id"]))
        conn.close()
        return redirect(url_for("index"))

    users = cur.execute("SELECT id, name FROM users ORDER BY name;").fetchall()

    stats = cur.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM users)            AS n_users,
          (SELECT COUNT(*) FROM matches)          AS n_matches,
          (SELECT COUNT(*) FROM predictions)      AS n_predictions
        """
    ).fetchone()

    conn.close()
    return render_template("index.html", users=users, stats=stats)


@app.route("/fixture")
def fixture():
    """
    Muestra el fixture con los pronósticos del usuario seleccionado,
    incluyendo estado del partido, puntos y stats de progreso.
    """
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return redirect(url_for("index"))

    conn = get_db_connection()
    user = conn.execute("SELECT id, name FROM users WHERE id = ?;", (user_id,)).fetchone()
    if not user:
        conn.close()
        return redirect(url_for("index"))

    rows = conn.execute(
        """
        SELECT 
            m.id AS match_id,
            m.group_name,
            m.stage,
            m.kickoff,
            m.home_team,
            m.away_team,
            m.home_score,
            m.away_score,
            p.home_pred,
            p.away_pred
        FROM matches m
        LEFT JOIN predictions p
          ON p.match_id = m.id AND p.user_id = ?
        ORDER BY m.kickoff, m.id;
        """,
        (user_id,),
    ).fetchall()
    conn.close()

    matches = []
    for r in rows:
        pts = None
        if r["home_pred"] is not None and r["away_pred"] is not None:
            pts = compute_points(r["home_pred"], r["away_pred"], r["home_score"], r["away_score"])

        if r["home_score"] is None or r["away_score"] is None:
            status = "Pendiente"
        else:
            status = "Jugado"

        has_prediction = (r["home_pred"] is not None and r["away_pred"] is not None)

        matches.append(
            {
                "match_id": r["match_id"],
                "group_name": r["group_name"],
                "stage": r["stage"],
                "kickoff": r["kickoff"],
                "home_team": r["home_team"],
                "away_team": r["away_team"],
                "home_score": r["home_score"],
                "away_score": r["away_score"],
                "home_pred": r["home_pred"],
                "away_pred": r["away_pred"],
                "points": pts,
                "status": status,
                "has_prediction": has_prediction,
            }
        )

    # ===== Stats de progreso para el usuario =====
    n_matches = len(matches)
    n_pred = sum(1 for m in matches if m["has_prediction"])
    n_played = sum(1 for m in matches if m["status"] == "Jugado")
    n_scored = sum(1 for m in matches if m["points"] is not None)

    completion_pct = int(round(100 * n_pred / n_matches)) if n_matches > 0 else 0

    stats = {
        "n_matches": n_matches,
        "n_pred": n_pred,
        "n_played": n_played,
        "n_scored": n_scored,
        "completion_pct": completion_pct,
    }

    return render_template("fixture.html", user=user, matches=matches, stats=stats)



@app.route("/predict/<int:match_id>", methods=["GET", "POST"])
def predict(match_id):
    """
    Carga/edita pronóstico de un partido para un usuario.
    """
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return redirect(url_for("index"))

    conn = get_db_connection()
    user = conn.execute("SELECT id, name FROM users WHERE id = ?;", (user_id,)).fetchone()
    match = conn.execute("SELECT * FROM matches WHERE id = ?;", (match_id,)).fetchone()
    if not user or not match:
        conn.close()
        return redirect(url_for("index"))

    prediction = conn.execute(
        "SELECT * FROM predictions WHERE match_id = ? AND user_id = ?;",
        (match_id, user_id),
    ).fetchone()

    error = None

    if request.method == "POST":
        home_pred = request.form.get("home_pred", "").strip()
        away_pred = request.form.get("away_pred", "").strip()

        if home_pred == "" or away_pred == "":
            error = "Completá ambos resultados."
        else:
            try:
                hp = int(home_pred)
                ap = int(away_pred)

                if prediction:
                    conn.execute(
                        """
                        UPDATE predictions
                        SET home_pred = ?, away_pred = ?, created_at = CURRENT_TIMESTAMP
                        WHERE id = ?;
                        """,
                        (hp, ap, prediction["id"]),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO predictions (match_id, user_id, home_pred, away_pred)
                        VALUES (?, ?, ?, ?);
                        """,
                        (match_id, user_id, hp, ap),
                    )
                conn.commit()
                conn.close()
                return redirect(url_for("fixture", user_id=user_id))
            except ValueError:
                error = "Los goles deben ser números enteros."

    conn.close()
    return render_template("predict.html", user=user, match=match, prediction=prediction, error=error)


@app.route("/ranking")
def ranking():
    """
    Ranking global por usuario, sumando puntos de todos los partidos con resultado cargado.
    """
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT 
            u.id AS user_id,
            u.name AS user_name,
            p.home_pred,
            p.away_pred,
            m.home_score,
            m.away_score
        FROM predictions p
        JOIN users u ON u.id = p.user_id
        JOIN matches m ON m.id = p.match_id;
        """
    ).fetchall()
    conn.close()

    scores = {}
    for r in rows:
        pts = compute_points(r["home_pred"], r["away_pred"], r["home_score"], r["away_score"])
        if pts is None:
            continue  # partidos sin resultado todavía no suman
        uid = r["user_id"]
        if uid not in scores:
            scores[uid] = {"user_id": uid, "user_name": r["user_name"], "points": 0}
        scores[uid]["points"] += pts

    ranking_list = sorted(scores.values(), key=lambda x: (-x["points"], x["user_name"]))

    return render_template("ranking.html", ranking=ranking_list)

@app.route("/pools", methods=["GET", "POST"])
def pools():
    """
    Pantalla de ligas:
      - Lista las ligas en las que está el usuario.
      - Permite crear una liga nueva.
      - Permite unirse a una liga por código.
    """
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return redirect(url_for("index"))

    conn = get_db_connection()
    cur = conn.cursor()

    user = cur.execute("SELECT id, name FROM users WHERE id = ?;", (user_id,)).fetchone()
    if not user:
        conn.close()
        return redirect(url_for("index"))

    message = None
    message_type = "info"

    if request.method == "POST":
        action = request.form.get("action")

        # Crear liga
        if action == "create":
            pool_name = request.form.get("pool_name", "").strip()
            if pool_name:
                code = generate_pool_code()
                # Crear liga
                cur.execute(
                    "INSERT INTO pools (name, code) VALUES (?, ?);",
                    (pool_name, code),
                )
                pool_id = cur.lastrowid
                # Agregar usuario como miembro (owner)
                cur.execute(
                    """
                    INSERT INTO pool_members (pool_id, user_id, role)
                    VALUES (?, ?, 'owner');
                    """,
                    (pool_id, user_id),
                )
                conn.commit()
                message = f"Liga creada: {pool_name} (código: {code})"
                message_type = "success"
            else:
                message = "Poné un nombre para la liga."
                message_type = "danger"

        # Unirse a liga
        elif action == "join":
            code = request.form.get("code", "").strip().upper()
            if code:
                pool = cur.execute(
                    "SELECT id, name, code FROM pools WHERE code = ?;",
                    (code,),
                ).fetchone()
                if pool:
                    # Insertar miembro si no estaba ya
                    try:
                        cur.execute(
                            """
                            INSERT INTO pool_members (pool_id, user_id, role)
                            VALUES (?, ?, 'member');
                            """,
                            (pool["id"], user_id),
                        )
                        conn.commit()
                        message = f"Te uniste a la liga: {pool['name']}."
                        message_type = "success"
                    except sqlite3.IntegrityError:
                        message = "Ya estás en esa liga."
                        message_type = "info"
                else:
                    message = "No existe ninguna liga con ese código."
                    message_type = "danger"
            else:
                message = "Ingresá un código de liga."
                message_type = "danger"

    # Listar ligas del usuario
    pools = cur.execute(
        """
        SELECT p.id, p.name, p.code, p.created_at, pm.role
        FROM pools p
        JOIN pool_members pm ON pm.pool_id = p.id
        WHERE pm.user_id = ?
        ORDER BY p.created_at;
        """,
        (user_id,),
    ).fetchall()

    conn.close()

    return render_template(
        "pools.html",
        user=user,
        pools=pools,
        message=message,
        message_type=message_type,
    )
@app.route("/pools/<int:pool_id>/ranking")
def pool_ranking(pool_id):
    """
    Ranking dentro de una liga (pool).
    """
    user_id = request.args.get("user_id", type=int)  # opcional, solo para volver al fixture
    conn = get_db_connection()
    cur = conn.cursor()

    pool = cur.execute(
        "SELECT id, name, code FROM pools WHERE id = ?;",
        (pool_id,),
    ).fetchone()

    if not pool:
        conn.close()
        return "Liga no encontrada", 404

    # Traer predicciones SOLO de usuarios miembros de la liga
    rows = cur.execute(
        """
        SELECT
            u.id AS user_id,
            u.name AS user_name,
            p.home_pred,
            p.away_pred,
            m.home_score,
            m.away_score
        FROM pool_members pm
        JOIN users u ON u.id = pm.user_id
        JOIN predictions p ON p.user_id = u.id
        JOIN matches m ON m.id = p.match_id
        WHERE pm.pool_id = ?;
        """,
        (pool_id,),
    ).fetchall()
    conn.close()

    scores = {}
    for r in rows:
        pts = compute_points(r["home_pred"], r["away_pred"], r["home_score"], r["away_score"])
        if pts is None:
            continue
        uid = r["user_id"]
        if uid not in scores:
            scores[uid] = {"user_id": uid, "user_name": r["user_name"], "points": 0}
        scores[uid]["points"] += pts

    ranking_list = sorted(scores.values(), key=lambda x: (-x["points"], x["user_name"]))

    return render_template(
        "pool_ranking.html",
        pool=pool,
        ranking=ranking_list,
        user_id=user_id,
    )


# -----------------------------
# Panel admin para resultados
# -----------------------------
@app.route("/admin/matches")
def admin_matches():
    """
    Lista de partidos para cargar/editar resultados reales.
    (Sin auth: asumimos que es uso privado).
    """
    conn = get_db_connection()
    matches = conn.execute("SELECT * FROM matches ORDER BY kickoff, id;").fetchall()
    conn.close()
    return render_template("admin_matches.html", matches=matches)


@app.route("/admin/matches/<int:match_id>", methods=["GET", "POST"])
def admin_edit_match(match_id):
    conn = get_db_connection()
    match = conn.execute("SELECT * FROM matches WHERE id = ?;", (match_id,)).fetchone()
    if not match:
        conn.close()
        return "Partido no encontrado", 404

    error = None

    if request.method == "POST":
        home_score = request.form.get("home_score", "").strip()
        away_score = request.form.get("away_score", "").strip()

        if home_score == "" or away_score == "":
            error = "Completá ambos resultados (podés usar 0)."
        else:
            try:
                hs = int(home_score)
                as_ = int(away_score)
                conn.execute(
                    """
                    UPDATE matches
                    SET home_score = ?, away_score = ?
                    WHERE id = ?;
                    """,
                    (hs, as_, match_id),
                )
                conn.commit()
                conn.close()
                return redirect(url_for("admin_matches"))
            except ValueError:
                error = "Los goles deben ser números enteros."

    conn.close()
    return render_template("admin_edit_match.html", match=match, error=error)

@app.before_first_request
def initialize_database():
    """
    Esto se ejecuta una sola vez cuando llega el primer request
    (funciona en Render con gunicorn).
    """
    try:
        print("🔧 Inicializando base de datos en before_first_request...")
        init_db()
        ensure_pool_tables()
        print("✅ Base de datos lista.")
    except Exception as e:
        print("❌ Error inicializando la base:", e)



# -----------------------------
# Main
# -----------------------------
init_db()
ensure_pool_tables()

if __name__ == "__main__":
    app.run(debug=True)