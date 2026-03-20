import subprocess
import os

def run(cmd):
    print(f"\n>>> {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print("❌ Ошибка выполнения команды.")
        input("Нажмите Enter для выхода...")
        exit(1)

def main():
    branch = input("Введите название ветки Codex (например jlwt0o-codex/fix-critical-visual-inconsistencies): ").strip()

    print("\n=== Обновляем origin ===")
    run("git fetch origin")

    print("\n=== Обновляем main ===")
    run("git checkout main")
    run("git pull origin main")

    print("\n=== Переходим в ветку Codex ===")
    run(f"git checkout {branch}")

    print("\n=== Мержим main в ветку Codex ===")
    merge_result = subprocess.run("git merge origin/main", shell=True)

    if merge_result.returncode != 0:
        print("\n⚠ Обнаружены конфликты. Принимаем изменения Codex (ours)...")
        run("git checkout --ours .")
        run("git add .")
        run('git commit -m "Resolved conflicts keeping Codex changes"')
    else:
        print("✅ Конфликтов нет.")

    print("\n=== Пушим ветку ===")
    run(f"git push origin {branch} --force")

    print("\n🎯 Готово. PR можно мержить.")
    input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    main()
