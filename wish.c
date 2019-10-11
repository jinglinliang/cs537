#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define TOK_BUFSIZE 64
#define TOK_DELIM " \t\r\n\a"


char *search_path[100];



void printError() {
	char error_message[30] = "An error has occurred\n";
	write(STDERR_FILENO, error_message, strlen(error_message));
}




char **get_args(char *line) {

	/*char *line = NULL;
	size_t len = 0;
	//ssize_t read;

	getline(&line, &len, stream);*/

	int bufsize = TOK_BUFSIZE, position = 0;
	char **tokens = malloc(bufsize * sizeof(char*));
	char *token;
	//**tokens_backup;

	token = strtok(line, TOK_DELIM);

	while (token != NULL) {
		tokens[position] = token;
		position++;
		//printf("%s,\n", token);

		if (position >= bufsize) {
			bufsize += TOK_BUFSIZE;
			//tokens_backup = tokens;
			tokens = realloc(tokens, bufsize * sizeof(char*));

			/*if (!tokens) {
				free(tokens_backup);
				fprintf(stderr, "lsh: allocation error\n");
				exit(EXIT_FAILURE);
			}*/
		}

		token = strtok(NULL, TOK_DELIM);
	}

	tokens[position] = NULL;
	return tokens;
}






void my_cd(char **args);
void my_path(char **args);
void my_exit(char **args);

char *builtin_str[] = {"cd", "path", "exit"};

void (*builtin_func[]) (char **) = {&my_cd, &my_path, &my_exit};

void my_cd(char **args) {
	if (args[1] == NULL || args[2] != NULL) {
		printError();
	}
	else {
		if (chdir(args[1]) != 0) {
			printError();
		}
	}
	//return 1;
}

void my_exit(char **args) {
	if (args[1] != NULL)
		printError();
	else
		exit(0);
}

void my_path(char **args) {

	int i = 0;
	while (args[i] != NULL) {
		search_path[i] = args[i+1];
		i++;
	}
  //return 0;
}





int execute(char **args) {

	pid_t pid;
	//int status;
	/*int i;

	if (args[0] == NULL) {
	// An empty command was entered.
	return 1;
	}*/

	for (int i = 0; i < 3; i++) {
		if (strcmp(args[0], builtin_str[i]) == 0) {
		  (*builtin_func[i])(args);
		  return 0;
		}
	}

	pid = fork();
	if (pid == 0) {
		// Child process
		int i = 0;
		char *exe_path;
		do{
			if (search_path[i] == NULL)
				break;
			exe_path = strdup(search_path[i]);
			exe_path = strcat(exe_path, "/");
			exe_path = strcat(exe_path, args[0]);
			i++;
		} while (execv(exe_path, args) == -1);

		printError();
		return -1;
	}
	else { return 1; }
	/*else if (pid < 0) {
	// Error forking
	perror("lsh");
	} else {
	// Parent process
	do {
	waitpid(pid, &status, WUNTRACED);
	} while (!WIFEXITED(status) && !WIFSIGNALED(status));
	}*/
}



int main(int argc, char **argv)
{
	char **args;
	int status;

	char *line = NULL;
	size_t len = 0;
	ssize_t read;
	FILE *stream;

	search_path[0] = "/bin";

	if (argc == 1) { //normal mode
		do {
			printf("wish> ");

			//args = get_args(stdin);
			status = execute(args);
			if (status == -1){
				exit(0);
			}
			else if (status == 1) {
				wait(NULL);
			}
			//free(line);
			//free(args);
		} while (1);
	}

	else if (argc == 2) { //batch mode

		stream = fopen(argv[1], "r");
		if (stream == NULL) {
			printError();
			exit(1);
		}
		while ((read = getline(&line, &len, stream)) != -1) {
			args = get_args(line);
			status = execute(args);
			if (status == -1){
				exit(0);
			}
			else if (status == 1) {
				wait(NULL);
			}
		}
		exit(0);
	}

	else {
		exit(1);
	}


}
