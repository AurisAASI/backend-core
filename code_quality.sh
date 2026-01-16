#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_step() {
    echo -e "${BLUE}$1${NC}"
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_error() {
    echo -e "${RED}$1${NC}"
}

print_warning() {
    echo -e "${YELLOW}$1${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  lint       Check code quality (isort, blue)"
    echo "  lint-fix   Check code quality and fix formatting issues"
    echo "  format     Format code (isort, blue)"
    echo "  format-fix Same as format (already fixes code)"
    echo "  test       Run tests with coverage"
    echo "  all        Run lint + test"
    echo "  all-fix    Run lint-fix + test (fix formatting then test)"
    echo "  help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 lint          # Check code quality"
    echo "  $0 lint-fix      # Check and fix formatting issues"
    echo "  $0 format        # Format code"
    echo "  $0 test          # Run tests"
    echo "  $0 all           # Run lint and tests"
    echo "  $0 all-fix       # Fix formatting and run tests"
}

# Function to run linting checks
run_lint() {
    print_step "🔍 Running code quality checks..."
    
    print_step "📋 Checking import sorting with isort..."
    if isort --check-only --diff --profile black src tests; then
        print_success "✅ Import sorting check passed"
    else
        print_error "❌ Import sorting check failed"
        return 1
    fi
    
    print_step "🎨 Checking code formatting with blue..."
    if blue --check --diff src tests; then
        print_success "✅ Code formatting check passed"
    else
        print_error "❌ Code formatting check failed"
        return 1
    fi
    
    print_success "✅ All linting checks passed!"
}

# Function to format code
run_format() {
    print_step "🎨 Formatting code..."
    
    print_step "📋 Sorting imports with isort..."
    isort --profile black src tests
    print_success "✅ Imports sorted"
    
    print_step "🎨 Formatting code with blue..."
    blue src tests
    print_success "✅ Code formatted"
    
    print_success "✅ Code formatting completed!"
}

# Function to run tests
run_test() {
    print_step "🧪 Running tests with coverage..."
    
    if pytest --cov=src --cov-report=html --cov-report=xml --cov-report=term-missing -v tests/; then
        print_success "✅ All tests passed!"
        print_step "📊 Coverage report generated in htmlcov/index.html"
    else
        print_error "❌ Tests failed"
        return 1
    fi
}

# Function to run all checks
run_all() {
    print_step "🚀 Running full code quality pipeline..."
    
    if run_lint && run_test; then
        print_success "🎉 All checks passed! Code is ready for deployment."
    else
        print_error "💥 Some checks failed. Please fix the issues before proceeding."
        return 1
    fi
}

# Function to run linting checks with fixing
run_lint_fix() {
    print_step "🔍 Running code quality checks with auto-fix..."
    
    print_step "📋 Fixing import sorting with isort..."
    isort --profile black src tests
    print_success "✅ Imports sorted and fixed"
    
    print_step "🎨 Fixing code formatting with blue..."
    blue src tests
    print_success "✅ Code formatted and fixed"
    
    print_success "✅ All linting checks passed with fixes applied!"
}

# Function to run all checks with fixing
run_all_fix() {
    print_step "🚀 Running full code quality pipeline with auto-fix..."
    
    if run_lint_fix && run_test; then
        print_success "🎉 All checks passed with fixes applied! Code is ready for deployment."
    else
        print_error "💥 Some checks failed. Please review the issues above."
        return 1
    fi
}

# Main script logic
case "${1:-help}" in
    lint)
        run_lint
        ;;
    lint-fix)
        run_lint_fix
        ;;
    format)
        run_format
        ;;
    format-fix)
        print_step "🎨 Format-fix is the same as format (already fixes code)"
        run_format
        ;;
    test)
        run_test
        ;;
    all)
        run_all
        ;;
    all-fix)
        run_all_fix
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        print_error "Unknown option: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac